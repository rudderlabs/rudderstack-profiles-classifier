from Processor import Processor


class SnowflakeProcessor(Processor):
    def _call_procedure(self, *args, **kwargs):
        """Calls the given procedure on the snowpark session

        Args:
            session (snowflake.snowpark.Session): Snowpark session object to access the warehouse
            args (list): List of arguments to be passed to the procedure

        Returns:
            Results of the procedure call
        """
        session = kwargs.get("session", None)
        if session is None:
            raise Exception("Session object not found")
        return session.call(*args)
