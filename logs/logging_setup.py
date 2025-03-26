import logging.config
import yaml
import uuid
import datetime
from typing import Optional

def setup_logging(config_path: Optional[str]='../logs/logging_config.yaml', log_file_path: Optional[str]='../logs/app.log') -> logging.Logger:

    # Generate run ID
    run_id = str(uuid.uuid4())[:8]  # Generate a unique run ID

    # Add a separator line at the start of each run
    separator = f'\n{"=" * 60}\nNew Run: {datetime.datetime.now()} | Run ID: {run_id}\n{"=" * 60}\n'
    with open(log_file_path, 'a', encoding='utf-8') as f:
        f.write(separator)

    # Load log config from YAML file
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # Inject run_id into log records globally
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.run_id = run_id  # Add run_id dynamically
        return record
    logging.setLogRecordFactory(record_factory)

    # Apply logging configuration
    logging.config.dictConfig(config)

    return logging.getLogger(__name__)

logger = setup_logging()



#if __name__ != '__main__':
    #logger = setup_logging()
