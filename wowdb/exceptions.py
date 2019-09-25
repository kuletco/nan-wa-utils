class DBError(BaseException):
    pass


class DBAccessError(DBError):
    pass


class DBTableError(DBError):
    pass
