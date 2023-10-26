import os

class Path:
    @staticmethod
    def make_path(file):
        """Function to create a path including the root"""
        path = os.path.join(
            os.getcwd(),
            file
        )
        return path