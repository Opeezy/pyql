class EmptyDataError(Exception):
    '''
    Thrown when query returns empty dataframe
    '''
    def __init__(self, *args: object) -> None:
        super().__init__(*args)