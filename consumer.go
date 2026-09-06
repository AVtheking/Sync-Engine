package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	connString      = "postgres://postgres:postgres@localhost:5434/syncengine?replication=database"
	slotName        = "sync_slot"
	publicationName = "sync_pub"
	outputPlugin    = "pgoutput"
)

type Consumer struct {
	relations     map[uint32]*pglogrepl.RelationMessageV2
	shapeRegistry *ShapeRegistry
	txBuffer      map[string][]LogEntries
}

func NewConsumer() *Consumer {
	return &Consumer{
		relations:     map[uint32]*pglogrepl.RelationMessageV2{},
		shapeRegistry: NewShapeRegistry(),
		txBuffer:      make(map[string][]LogEntries),
	}
}

func RunConsumer(ctx context.Context, consumer *Consumer) error {
	conn, err := pgconn.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	defer conn.Close(ctx)

	var pgErr *pgconn.PgError

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		if !(errors.As(err, &pgErr) && pgErr.Code == "42710") {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		// slot already exists, continue
	} else {
		log.Println("Replication slot created successfully")
	}

	err = pglogrepl.StartReplication(ctx, conn, slotName, 0, pglogrepl.StartReplicationOptions{PluginArgs: []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", publicationName),
	}})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}
	log.Println("Replication started successfully")

	return consumer.streamLoop(ctx, conn)
}

func (c *Consumer) streamLoop(ctx context.Context, conn *pgconn.PgConn) error {
	var confirmedLSN pglogrepl.LSN
	standbyInterval := 10 * time.Second
	nextStandbyDeadline := time.Now().Add(standbyInterval)

	for {
		if time.Now().After(nextStandbyDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: confirmedLSN})
			if err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			nextStandbyDeadline = time.Now().Add(standbyInterval)
		}

		rctx, cancel := context.WithDeadline(ctx, nextStandbyDeadline)
		rawMsg, err := conn.ReceiveMessage(rctx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}
		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				nextStandbyDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}
			if err := c.handleMessage(xld.WALData); err != nil {
				return err
			}
			confirmedLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

		}

	}

}

func (c *Consumer) handleMessage(walData []byte) error {
	msg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return fmt.Errorf("parse logical message : %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		c.relations[m.RelationID] = m
		log.Printf("RELATION %s.%s (%d columns)", m.Namespace, m.RelationName, len(m.Columns))

	case *pglogrepl.BeginMessage:
		log.Printf("BEGIN (commit lsn=%s)", m.FinalLSN)
		c.txBuffer = make(map[string][]LogEntries)

	case *pglogrepl.InsertMessageV2:
		rel := c.relations[m.RelationID]
		log.Printf("INSERT %s: %v", rel.RelationName, decodeTuple(rel, m.Tuple))
		row := decodeTuple(rel, m.Tuple)
		c.txBuffer[rel.RelationName] = append(c.txBuffer[rel.RelationName], LogEntries{
			Ops:   "insert",
			Key:   row["id"],
			Value: row,
		})

	case *pglogrepl.UpdateMessageV2:
		rel := c.relations[m.RelationID]
		log.Printf("UPDATE %s: old=%v new=%v", rel.RelationName,
			decodeTuple(rel, m.OldTuple), decodeTuple(rel, m.NewTuple))
		row := decodeTuple(rel, m.NewTuple)
		c.txBuffer[rel.RelationName] = append(c.txBuffer[rel.RelationName], LogEntries{
			Ops:   "update",
			Key:   row["id"],
			Value: row,
		})

	case *pglogrepl.DeleteMessageV2:
		rel := c.relations[m.RelationID]
		log.Printf("DELETE %s: %v", rel.RelationName, decodeTuple(rel, m.OldTuple))
		row := decodeTuple(rel, m.OldTuple)
		c.txBuffer[rel.RelationName] = append(c.txBuffer[rel.RelationName], LogEntries{
			Ops:   "delete",
			Key:   row["id"],
			Value: row,
		})
	case *pglogrepl.CommitMessage:
		log.Printf("COMMIT (lsn=%s)", m.CommitLSN)

		for table, entries := range c.txBuffer {
			c.shapeRegistry.GetOrCreate(table).AppendEntries(entries)
		}

		c.txBuffer = nil
	default:
		log.Printf("other message: %T", msg)
	}
	return nil
}

func decodeTuple(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData) map[string]string {
	row := make(map[string]string)
	if tuple == nil {
		return row
	}

	for i, col := range tuple.Columns {
		name := rel.Columns[i].Name
		switch col.DataType {
		case 't': //text-encoded
			row[name] = string(col.Data)
		case 'n': //null
			row[name] = "NULL"
		case 'u': //TOASTed value unchanged, not sent (only on update)
			row[name] = "(unchanged)"
		}
	}
	return row
}
