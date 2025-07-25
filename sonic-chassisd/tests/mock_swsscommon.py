STATE_DB = ''


class Table:
    def __init__(self, *argv):
        self.db_or_pipe = argv[0]
        self.table_name = argv[1]
        self.mock_dict = {}

    def _del(self, key):
        if key in self.mock_dict:
            del self.mock_dict[key]
        pass

    def set(self, key, fvs):
        if isinstance(fvs, list):
            self.mock_dict[key] = dict(fvs)
        elif hasattr(fvs, 'fv_dict'):
            self.mock_dict[key] = fvs.fv_dict
        else:
            raise ValueError("Unsupported format for field-value pairs.")

    def get(self, key):
        if key in self.mock_dict:
            return [True, tuple(self.mock_dict[key].items())]
        return None

    def hget(self, key, field):
        if key not in self.mock_dict or field not in self.mock_dict[key]:
            return [False, None]

        return [True, self.mock_dict[key][field]]

    def hset(self, key, field, value):
        if key not in self.mock_dict:
            self.mock_dict[key] = {}

        self.mock_dict[key][field] = value

    def hdel(self, key, field):
        if key not in self.mock_dict or field not in self.mock_dict:
            return

        del self.mock_dict[key][field]

    def getKeys(self):
        return list(self.mock_dict)

    def size(self):
        return len(self.mock_dict)

class FieldValuePairs:
    def __init__(self, fvs):
        self.fv_dict = dict(fvs)
        pass

class Select:
    OBJECT = 0
    TIMEOUT = 1

    def addSelectable(self, selectable):
        pass

    def removeSelectable(self, selectable):
        pass

    def select(self, timeout=-1, interrupt_on_signal=False):
        return self.TIMEOUT, None

class SubscriberStateTable(Table):

    def pop(self):
        return None

    def pops(self):
        return None

    def getDbConnector(self):
        return MockDbConnector()


class MockDbConnector:

    def getDbName(self):
        return 'CHASSIS_STATE_DB'

class RedisPipeline:
    def __init__(self, db):
        self.db = db

    def loadRedisScript(self, script):
        self.script = script
        self.script_mock_sha = 'd79033d1cab85249929e8c069f6784474d71cc43'
        return self.script_mock_sha

class ConfigDBConnector:

    def connect(*args, **kwargs):
        pass

    def get_table(*args, **kwargs):
        pass
