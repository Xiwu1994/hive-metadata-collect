import happybase


class HbaseUtil(object):
    connection = happybase.Connection(host="host", port=9090)
    table = connection.table("hive_metadata_collect")

    @staticmethod
    def put(row_key, data):
        HbaseUtil.table.put(row_key, data)
