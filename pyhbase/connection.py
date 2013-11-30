# TODO(mjrusso) - discuss PEP-8 compliance with hammer
#                 (4 space indents, max 80 char line width, etc.)

import async
import avro.ipc as ipc
import avro.protocol as protocol
import os
import sys

from functools import partial, wraps

# TODO(hammer): Figure the canonical place to put this file
PROTO_FILE = os.path.join(os.path.dirname(__file__), 'schema/hbase.avpr')
PROTOCOL = protocol.parse(open(PROTO_FILE).read())

def retry_wrapper(fn):
  """a decorator to add retry symantics to any method that uses hbase"""
  @wraps(fn)
  def f(self, *args, **kwargs):
    try:
      return fn(self, *args, **kwargs)
    except:
      try:
        self.close()
      except:
        pass
      self.make_connection()
      return fn(self, *args, **kwargs)
  return f


class AbstractConnection(object):
  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.client = None
    self.requestor = None
    self.make_connection()

  def make_connection(self, retry=2):
    """Establishes the underlying connection to HBase."""
    while retry:
      retry -= 1
      try:
        self.client = self.TRANSCEIVER(self.host, self.port)
        self.requestor = self.REQUESTOR(PROTOCOL, self.client)
        return
      except:
        pass
    exceptionType, exception, tracebackInfo = sys.exc_info()
    raise exception

  # TODO(hammer): classify these methods like the HBase shell

  #
  # Metadata
  #

  @retry_wrapper
  def list_tables(self, **kwargs):
    """Grab table information."""
    return self.request("listTables", {}, **kwargs)

  @retry_wrapper
  def get_hbase_version(self, **kwargs):
    """Get HBase version."""
    return self.request("getHBaseVersion", {}, **kwargs)

  def get_cluster_status(self, **kwargs):
    """Get cluster status."""
    return self.request("getClusterStatus", {}, **kwargs)

  @retry_wrapper
  def describe_table(self, table, **kwargs):
    """Grab table information."""
    return self.request("describeTable", {"table": table}, **kwargs)

  @retry_wrapper
  def describe_family(self, table, family, **kwargs):
    """Grab family information."""
    return self.request("describeFamily", {"table": table, "family": family},
                        **kwargs)

  @retry_wrapper
  def is_table_enabled(self, table, **kwargs):
    """Determine if a table is enabled."""
    return self.request("isTableEnabled", {"table": table}, **kwargs)

  @retry_wrapper
  def table_exists(self, table, **kwargs):
    """Determine if a table exists."""
    return self.request("tableExists", {"table": table}, **kwargs)

  #
  # Get
  #

  # TODO(hammer): Figure out how to get binary keys
  # TODO(hammer): Do this parsing logic in pyhbase-cli?
  @retry_wrapper
  def get(self, table, row, *columns, **kwargs):
    get = {"row": row}
    columns = [len(column) > 1 and {"family": column[0], "qualifier": column[1]} or {"family": column[0]}
               for column in map(lambda s: s.split(":", 1), columns)]
    if columns: get["columns"] = columns
    params = {"table": table, "get": get}
    return self.request("get", params, **kwargs)

  @retry_wrapper
  def exists(self, table, row, *columns, **kwargs):
    get = {"row": row}
    columns = [len(column) > 1 and {"family": column[0], "qualifier": column[1]} or {"family": column[0]}
               for column in map(lambda s: s.split(":", 1), columns)]
    if columns: get["columns"] = columns
    params = {"table": table, "get": get}
    return self.request("exists", params, **kwargs)

  #
  # Put
  #

  # TODO(hammer): Figure out how to incorporate timestamps
  # TODO(hammer): Do this parsing logic in pyhbase-cli?
  @retry_wrapper
  def put(self, table, row, *column_values, **kwargs):
    put = {"row": row}
    column_values = [{"family": column.split(":", 1)[0], "qualifier": column.split(":", 1)[1], "value": value}
                     for column, value in zip(column_values[::2], column_values[1::2])]
    put["columnValues"] = column_values
    params = {"table": table, "put": put}
    return self.request("put", params, **kwargs)

  @retry_wrapper
  def incr(self, table, row, *column_and_amount, **kwargs):
    family, qualifier = column_and_amount[0].split(":", 1)
    amount = 1
    if len(column_and_amount) > 1:
      amount = column_and_amount[1]
    return self.request("incrementColumnValue", {"table": table, "row": row, "family": family, "qualifier": qualifier, "amount": int(amount), "writeToWAL": True}, **kwargs)

  #
  # Delete
  #

  @retry_wrapper
  def delete(self, table, row, *columns, **kwargs):
    delete = {"row": row}
    columns = [len(column) > 1 and {"family": column[0], "qualifier": column[1]} or {"family": column[0]}
               for column in map(lambda s: s.split(":", 1), columns)]
    if columns: delete["columns"] = columns
    params = {"table": table, "delete": delete}
    return self.request("delete", params, **kwargs)

  #
  # Scan
  #

  def _init_scan(self, table, number_of_rows, start_row=None, stop_row=None,
                 columns=None, timestamp=None):

    if columns:
      columns = [{"family": column[0], "qualifier": column[1]}
                 if len(column) > 1 else {"family": column[0]}
                 for column in map(lambda s: s.split(":", 1), columns)]

    params = {
      "table": table,
      "scan": {
        "startRow": start_row,
        "stopRow": stop_row,
        "columns": columns,
        "timestamp": timestamp
        },
      }

    return params


class HBaseConnection(AbstractConnection):

  TRANSCEIVER = ipc.HTTPTransceiver
  REQUESTOR = ipc.Requestor

  def request(self, message_name, request_datum, **kwargs):
    return self.requestor.request(message_name, request_datum)

  #
  # Administrative Operations
  # NB: Only available via synchronous connection
  #

  @retry_wrapper
  def create_table(self, table, *families):
    table_descriptor = {"name": table}
    families = [{"name": family} for family in families]
    if families: table_descriptor["families"] = families
    return self.request("createTable", {"table": table_descriptor})

  # NB: Delete is an asynchronous operation, so don't retry
  def alter(self, table, command, family):
    self.disable_table(table)
    if command == "add":
      self.request("addFamily", {"table": table, "family": {"name": family}})
    elif command == "delete":
      self.request("deleteFamily", {"table": table, "family": family})
    else:
      return "Unknown alter command: %s" % command
    self.flush(".META.")
    return self.enable_table(table)

  # TODO(hammer): Automatically major_compact .META. too?
  # NB: flush is an asynchronous operation, so don't retry
  def drop(self, table):
    self.disable_table(table)
    self.request("deleteTable", {"table": table})
    return self.flush(".META.")

  def truncate(self, table):
    table_descriptor = self.describe_table(table)
    self.drop(table)
    return self.request("createTable", {"table": table_descriptor})

  # NB: flush is an asynchronous operation, so don't retry
  def flush(self, table):
    self.request("flush", {"table": table})

  # NB: split is an asynchronous operation, so don't retry
  def split(self, table):
    self.request("split", {"table": table})

  @retry_wrapper
  def enable_table(self, table):
    return self.request("enableTable", {"table": table})

  @retry_wrapper
  def disable_table(self, table):
    return self.request("disableTable", {"table": table})

  #
  # Scan
  #

  @retry_wrapper
  def scan(self, table, number_of_rows, start_row=None, stop_row=None,
           columns=None, timestamp=None, **kwargs):

    params = self._init_scan(table, number_of_rows, start_row, stop_row,
                             columns, timestamp)

    scanner_id = self.request("scannerOpen", params)

    results = self.request("scannerGetRows", {
        "scannerId": scanner_id,
        "numberOfRows": int(number_of_rows),
        })

    self.request("scannerClose", {"scannerId": scanner_id})
    return results


class AsyncHBaseConnection(AbstractConnection):

  TRANSCEIVER = async.TornadoHTTPTransceiver
  REQUESTOR = async.TornadoRequestor

  def request(self, message_name, request_datum, **kwargs):
    return self.requestor.request(message_name, request_datum,
                                  kwargs['callback'])

  #
  # Scan
  #

  def _on_scanner_open(self, number_of_rows, callback, scanner_id):
    params = {
      "scannerId": scanner_id,
      "numberOfRows": int(number_of_rows),
      }
    self.request("scannerGetRows", params,
                 callback=partial(self._on_scanner_get_rows, scanner_id,
                                  callback))

  def _on_scanner_get_rows(self, scanner_id, callback, results):
    params = {"scannerId": scanner_id}
    self.request("scannerClose", params, callback=self._on_scanner_close)
    callback(results)

  def _on_scanner_close(self, response):
    pass # ignore the response

  @retry_wrapper
  def scan(self, table, number_of_rows, start_row=None, stop_row=None,
           columns=None, timestamp=None, **kwargs):

    params = self._init_scan(table, number_of_rows, start_row, stop_row,
                             columns, timestamp)

    self.request("scannerOpen", params,
                 callback=partial(self._on_scanner_open, number_of_rows,
                                  kwargs['callback']))
