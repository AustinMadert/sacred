#!/usr/bin/env python
# coding=utf-8

import json
from threading import Lock
import warnings

from sacred.commandline_options import cli_option
from sacred.observers.base import RunObserver
from sacred.serializer import flatten
from sacred.observers.sql_bases import Base, Experiment, Host, Run
import sqlalchemy as sqla

DEFAULT_SQL_PRIORITY = 40


# ############################# Observer #################################### #


class SqlObserverBase(RunObserver):
    @classmethod
    def create(cls, url, echo=False, priority=DEFAULT_SQL_PRIORITY):
        warnings.warn(
            "SqlObserver.create(...) is deprecated. Please use"
            " SqlObserver(...) instead.",
            DeprecationWarning,
        )
        return cls(url, echo, priority)

    def __init__(self, url, echo=False, priority=DEFAULT_SQL_PRIORITY):
        from sqlalchemy.orm import sessionmaker, scoped_session
        import sqlalchemy as sa

        engine = sa.create_engine(url, echo=echo)
        session_factory = sessionmaker(bind=engine)
        # make session thread-local to avoid problems with sqlite (see #275)
        session = scoped_session(session_factory)
        self.engine = engine
        self.session = session
        self.priority = priority
        self.run = None
        self.lock = Lock()

    @classmethod
    def create_from(cls, engine, session, priority=DEFAULT_SQL_PRIORITY):
        """Instantiate a SqlObserverBase with an existing engine and session."""
        self = cls.__new__(cls)  # skip __init__ call
        self.engine = engine
        self.session = session
        self.priority = priority
        self.run = None
        self.lock = Lock()
        return self

    def started_event(
        self, ex_info, command, host_info, start_time, config, meta_info, _id
    ):
        return self._add_event(
            ex_info,
            command,
            host_info,
            config,
            meta_info,
            _id,
            "RUNNING",
            start_time=start_time,
        )

    def queued_event(
        self, ex_info, command, host_info, queue_time, config, meta_info, _id
    ):
        return self._add_event(
            ex_info, command, host_info, config, meta_info, _id, "QUEUED"
        )

    def _add_event(
        self, ex_info, command, host_info, config, meta_info, _id, status, **kwargs
    ):
        from .sql_bases import Base, Experiment, Host, Run

        Base.metadata.create_all(self.engine)
        sql_exp = Experiment.get_or_create(ex_info, self.session)
        sql_host = Host.get_or_create(host_info, self.session)
        if _id is None:
            i = self.session.query(Run).order_by(Run.id.desc()).first()
            _id = 0 if i is None else i.id + 1

        self.run = Run(
            run_id=str(_id),
            config=json.dumps(flatten(config)),
            command=command,
            priority=meta_info.get("priority", 0),
            comment=meta_info.get("comment", ""),
            experiment=sql_exp,
            host=sql_host,
            status=status,
            **kwargs,
        )
        self.session.add(self.run)
        self.save()
        return _id or self.run.run_id

    def heartbeat_event(self, info, captured_out, beat_time, result):
        self.run.info = json.dumps(flatten(info))
        self.run.captured_out = captured_out
        self.run.heartbeat = beat_time
        self.run.result = result
        self.save()

    def completed_event(self, stop_time, result):
        self.run.stop_time = stop_time
        self.run.result = result
        self.run.status = "COMPLETED"
        self.save()

    def interrupted_event(self, interrupt_time, status):
        self.run.stop_time = interrupt_time
        self.run.status = status
        self.save()

    def failed_event(self, fail_time, fail_trace):
        self.run.stop_time = fail_time
        self.run.fail_trace = "\n".join(fail_trace)
        self.run.status = "FAILED"
        self.save()

    def resource_event(self, filename):
        from .sql_bases import Resource

        res = Resource.get_or_create(filename, self.session)
        self.run.resources.append(res)
        self.save()

    def artifact_event(self, name, filename, metadata=None, content_type=None):
        from .sql_bases import Artifact

        a = Artifact.create(name, filename)
        self.run.artifacts.append(a)
        self.save()

    def save(self):
        with self.lock:
            self.session.commit()

    def query(self, _id):
        from .sql_bases import Run

        run = self.session.query(Run).filter_by(id=_id).first()
        return run.to_json()

    def __eq__(self, other):
        if isinstance(other, SqlObserverBase):
            # fixme: this will probably fail to detect two equivalent engines
            return self.engine == other.engine and self.session == other.session
        return False


class SqlObserver(SqlObserverBase):


    def log_metrics(self, metrics_by_name: dict, info: dict) -> None:
        """Logs metrics recorded during an experiment to sql table.
    
        The function reformats the data in the metrics_by_name dictionary. The
        metrics table is then located and the data are committed to the table.
        
        Parameters
        ----------
            metrics_by_name : dict
                Contains keys for name, steps, timestamp, and value for logged
                metrics
                
            info : dict
                Not used in this implementation but included for compatibility
                with Mongodb observer object.
            
        Notes
        -----
            This method is used by the sacred experiment's log_scalar method and
            is not intended to be directly called.
        """
        # Retrieve the table.
        meta = sqla.MetaData(self.engine)
        table = sqla.Table('metrics', meta,
        sqla.Column('id', sqla.Integer, primary_key=True),
        sqla.Column('run_id', sqla.String(24)),
        sqla.Column('name', sqla.String(64)),
        sqla.Column('steps', sqla.Integer),
        sqla.Column('timestamps', sqla.DateTime),
        sqla.Column('values', sqla.Integer))
        # In case the table doesn't already exist.
        meta.create_all()

        # Keys are not used. Each value is a dictionary.
        metrics_lst = [v for _, v in metrics_by_name.items()]
        records = []

        for m in metrics_lst:
            metric = zip(m['steps'], m['timestamps'], m['values'])
            for step, time, value in metric:
                # Recreate each record using the metric table's formatting.
                record = {
                    'run_id': self.run.run_id,
                    'name': m['name'],
                    'steps': step,
                    'timestamps': time,
                    'values': value,
                }
                # Accumulate the records for multi-insert below.
                records.append(record)

        # Avoid empty rows in Metrics table on runs with errors.
        if not records:
            return None

        else:
            conn = self.engine.connect()
            conn.execute(table.insert(), records)


    def _add_event(self, ex_info, command, host_info, config, meta_info, _id, 
                   status, **kwargs) -> int:
        """A method to add run events to sacred experiments.
        This method is overwritten solely to add the creation of the metrics
        table via the Metrics class. The rest of the code remains unchanged.
                
        Returns
        -------
            int
                The unique identifier for the current experiment run.
        """
        class Metrics(Base):
            """This class sub-classes the Base object which is a sqlalchemy
            declarative base object. Every sub-class results in the creation of
            a table with the given column variables and table name.
            """
            __tablename__ = "metrics"
            __table_args__ = {'extend_existing': True}
            id = sqla.Column(sqla.Integer, primary_key=True)
            run_id = sqla.Column(sqla.String(24))
            name = sqla.Column(sqla.String(64))
            steps = sqla.Column(sqla.Integer)
            timestamp = sqla.Column(sqla.DateTime)
            value = sqla.Column(sqla.Integer)

            def to_json(self):
                pass

        Base.metadata.create_all(self.engine, checkfirst=True)
        sql_exp = Experiment.get_or_create(ex_info, self.session)
        sql_host = Host.get_or_create(host_info, self.session)
        if _id is None:
            i = self.session.query(Run).order_by(Run.id.desc()).first()
            _id = 0 if i is None else i.id + 1

        self.run = Run(
            run_id=str(_id),
            config=json.dumps(flatten(config)),
            command=command,
            priority=meta_info.get("priority", 0),
            comment=meta_info.get("comment", ""),
            experiment=sql_exp,
            host=sql_host,
            status=status,
            **kwargs,
        )
        self.session.add(self.run)
        self.save()

        return _id or self.run.run_id


# ######################## Commandline Option ############################### #


@cli_option("-s", "--sql")
def sql_option(args, run):
    """Add a SQL Observer to the experiment.

    The typical form is: dialect://username:password@host:port/database
    """
    run.observers.append(SqlObserver(args))
