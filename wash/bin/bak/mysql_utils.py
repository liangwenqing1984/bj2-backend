
class MySQLUtils(object):
    def __init__(self):
        super(MySQLUtils, self).__init__()

    @staticmethod
    def get_rs_as_dict(cursor):
        result = []
        desc = cursor.description

        for row in cursor.fetchall():
            dict = {}
            for col in range(len(desc)):
                dict[desc[col][0]] = row[col]
            result.append(dict)

        return result

