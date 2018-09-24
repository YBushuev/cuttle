package com.criteo.cuttle.timeseries

import com.criteo.cuttle._
import Internal._

import java.time._

import cats.Applicative
import cats.data.{NonEmptyList, OptionT}
import cats.implicits._

import io.circe._
import io.circe.syntax._

import doobie._
import doobie.implicits._

private[timeseries] object Database {

    import TimeSeriesUtils._
    import com.criteo.cuttle.Database._

    import intervals.{Interval, IntervalMap}

    val contextIdMigration: ConnectionIO[Unit] = {
        implicit val jobs: Set[TimeSeriesJob] = Set.empty
        val chunkSize = 1024 * 10
        val stream = sql"""SELECT id, "json" FROM timeseries_contexts"""
            .query[(String, Json)]
            .streamWithChunkSize(chunkSize)
        val insert = Update[(String, String)]("INSERT into tmp (id, new_id) VALUES (? , ?)")
        for {
            _ <- sql"CREATE TEMPORARY TABLE tmp (id VARCHAR(1000), new_id VARCHAR(1000))".update.run
            _ <- stream
                .chunkLimit(chunkSize)
                .evalMap { oldContexts =>
                    insert.updateMany(oldContexts.map {
                        case (id, json) =>
                            //TODO
                            (id, json.as[TimeSeriesContext].toOption.get.toId)
                    })
                }
                .compile
                .drain
            _ <- sql"""CREATE INDEX tmp_id ON tmp (id)""".update.run
            _ <-
                sql"""UPDATE timeseries_contexts
                 SET id =  (select tmp.new_id from tmp where timeseries_contexts.id = tmp.id) """.update.run
            _ <-
                sql"""UPDATE executions
                 SET context_id = (select tmp.new_id from tmp where executions.context_id = tmp.id)""".update.run
        } yield ()
    }

    val schema = List(
        sql"""
      CREATE TABLE timeseries_state (
        state       JSONB NOT NULL,
        date        TIMESTAMP NOT NULL
      );

      CREATE INDEX timeseries_state_by_date ON timeseries_state (date);

      CREATE TABLE timeseries_contexts (
        id          VARCHAR(1000) NOT NULL,
        "json"      JSONB NOT NULL,
        ctx_range   int8range NOT NULL,
        backfill_id CHAR(36) NULL,
        PRIMARY KEY (id)
      );

      CREATE INDEX timeseries_contexts_by_range  ON timeseries_contexts USING GIST (ctx_range);

      CREATE TABLE timeseries_backfills (
        id          CHAR(36) NOT NULL,
        "name"      VARCHAR(200) NOT NULL,
        description TEXT,
        jobs        TEXT NOT NULL,
        priority    SMALLINT NOT NULL,
        "start"     TIMESTAMP NOT NULL,
        "end"       TIMESTAMP NOT NULL,
        created_at  TIMESTAMP NOT NULL,
        created_by  VARCHAR(100) NOT NULL,
        status      VARCHAR(100) NOT NULL,
        PRIMARY KEY (id)
      );

      CREATE INDEX timeseries_backfills_by_date ON timeseries_backfills (created_at);
      CREATE INDEX timeseries_backfills_by_status ON timeseries_backfills (status);
    """.update.run,
        contextIdMigration
    )

    val doSchemaUpdates: ConnectionIO[Unit] = utils.updateSchema("timeseries", schema)

    def sqlGetContextsBetween(start: Option[Instant], end: Option[Instant]): Fragment =
        sql"""
      SELECT id, "json" FROM timeseries_contexts
      WHERE  ctx_range && int8range(${start.map(_.getEpochSecond).getOrElse(0L)}, ${end.map(_.getEpochSecond).getOrElse(Long.MaxValue)} )
    """

    def serializeContext(context: TimeSeriesContext): ConnectionIO[String] = {
        val id = context.toId
        sql"""
      INSERT INTO timeseries_contexts (id, "json", ctx_range, backfill_id)
      VALUES (
        ${id},
        ${context.asJson} :: JSONB,
        int8range(${context.start.getEpochSecond},${context.end.getEpochSecond}),
        ${context.backfill.map(_.id)}
      ) ON CONFLICT ON CONSTRAINT timeseries_contexts_pkey DO UPDATE SET
             "json" = EXCLUDED."json",
              "ctx_range" = EXCLUDED.ctx_range,
              "backfill_id" = EXCLUDED.backfill_id
    """.update.run *> Applicative[ConnectionIO].pure(id)
    }

    def deserializeState(implicit jobs: Set[Job[TimeSeries]]): ConnectionIO[Option[State]] = {
        type StoredState = List[(String, List[(Interval[Instant], JobState)])]
        OptionT {
            sql"SELECT state FROM timeseries_state ORDER BY date DESC LIMIT 1"
                .query[Json]
                .option
        }.map(_.as[StoredState].toOption.get.flatMap {
            case (jobId, st) =>
                jobs.find(_.id == jobId).map(job => job -> IntervalMap(st: _*))
        }.toMap)
            .value
    }

    def serializeState(state: State): ConnectionIO[Int] = {
        import JobState.{Done, Todo}

        val now = Instant.now()
        val stateJson = state.toList.map {
            case (job, im) =>
                (job.id, im.toList.filter {
                    case (_, jobState) =>
                        jobState match {
                            case Done(_) => true
                            case Todo(_) => true
                            case _ => false
                        }
                })
        }.asJson
        sql"INSERT INTO timeseries_state (state, date) VALUES (${stateJson} ::JSONB , ${now})".update.run
    }

    def queryBackfills(where: Option[Fragment] = None) = {
        val select =
            sql"""SELECT id, "name", description, jobs, priority, "start", "end", created_at, status, created_by
            FROM timeseries_backfills"""
        where
            .map(select ++ sql" WHERE " ++ _)
            .getOrElse(select)
            .query[(String, String, String, String, Int, Instant, Instant, Instant, String, String)]
    }

    def getBackfillById(id: String): ConnectionIO[Option[Json]] = {
        val select =
            sql"""SELECT id, "name", description, jobs, priority, "start", "end", created_at, status, created_by
            FROM timeseries_backfills WHERE id=$id"""
        select
            .query[(String, String, String, String, Int, Instant, Instant, Instant, String, String)]
            .option
            .map(_.map {
                case (id, name, description, jobs, priority, start, end, created_at, status, created_by) =>
                    Json.obj(
                        "id" -> id.asJson,
                        "name" -> name.asJson,
                        "description" -> description.asJson,
                        "jobs" -> jobs.asJson,
                        "priority" -> priority.asJson,
                        "start" -> start.asJson,
                        "end" -> end.asJson,
                        "created_at" -> created_at.asJson,
                        "status" -> status.asJson,
                        "created_by" -> created_by.asJson
                    )
            })
    }

    def getExecutionLogsForBackfill(id: String): ConnectionIO[Seq[ExecutionLog]] =
        sql"""
        SELECT e.id, job, start_time, end_time, c.json AS context, success, e.waiting_seconds
          FROM executions e
        JOIN timeseries_contexts c
          ON  c.id = e.context_id
        WHERE c.backfill_id=$id
        ORDER BY c.id DESC
        """
            .query[(String, String, Instant, Instant, Json, ExecutionStatus, Int)]
            .to[List]
            .map(_.map {
                case (id, job, startTime, endTime, context, status, waitingSeconds) =>
                    ExecutionLog(id, job, Some(startTime), Some(endTime), context, status, waitingSeconds = waitingSeconds)
            })

    def createBackfill(backfill: Backfill) =
        sql"""INSERT INTO timeseries_backfills (id, "name", description, jobs, priority, "start", "end", created_at, status, created_by)
          VALUES (${backfill.id},
                  ${backfill.name},
                  ${backfill.description},
                  ${backfill.jobs.map(_.id).mkString(",")},
                  ${backfill.priority},
                  ${backfill.start},
                  ${backfill.end},
                  ${Instant.now()},
                  ${backfill.status},
                  ${backfill.createdBy}
                 )""".update.run

    def setBackfillStatus(ids: Set[String], status: String) =
        (
            sql"UPDATE timeseries_backfills SET status = $status WHERE " ++
                Fragments.in(fr"id", NonEmptyList.fromListUnsafe(ids.toList))
            ).update.run
}
