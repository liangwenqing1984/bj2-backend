
class DQCException(Exception):
    def __init__(self, exception_info):
        super().__init__(self)
        self.info = exception_info

    def __str__(self):
        return self.info


class MaskException(Exception):
    def __init__(self, exception_info):
        super().__init__(self)
        self.info = exception_info

    def __str__(self):
        return self.info
