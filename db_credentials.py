class DbCredentials:
    def __init__(self, db_id, db_pass):
        self.db_id = db_id
        self.db_pass = db_pass

    def to_dict(self):
        return {"db_id": self.db_id,
                "db_pass": self.db_pass}
