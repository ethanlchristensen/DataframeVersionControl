import pandas as pd


class VersionControlledDataframe:
    def __init__(self):
        self.commands = {
            "hello": self.hello,
            "load": self.load,
            "view": self.view,
            "filter": self.filter_where,
            "rollback": self.rollback,
            "drop": self.drop
        }

        self.data = {
            "frames": {
                "dataframe": None,
                "_dataframe": None,
                "__dataframe": None,
            }
        }

    def hello(self):
        response = "Hello World!"
        return response
    
    def load(self):
        file_path = input(" : enter file path to csv: ")
        try:
            dataframe = pd.read_csv(file_path, low_memory=False)
            self.data["frames"]["dataframe"] = dataframe
            response = "data successfully loaded!"
            return response
        except:
            response = "error. data not loaded!"
            return response
    
    def set_frames(self, df1, df2, df3):
        self.data.get("frames")["dataframe"] = df1
        self.data.get("frames")["_dataframe"] = df2
        self.data.get("frames")["__dataframe"] = df3

    def is_loaded(self):
        return self.data.get("frames").get("dataframe") is not None

    def view(self):
        if not self.is_loaded(): return "no data loaded!"
        print(self.data.get("frames").get("dataframe").head())

    def _bubble(self, bubbleframe):
        v1 = self.data.get("frames").get("dataframe")
        v2 = self.data.get("frames").get("_dataframe")

        self.set_frames(
            bubbleframe,
            v1,
            v2
        )

    def filter_where(self):
        if not self.is_loaded(): return "no data loaded!"
        current_dataframe = self.data.get("frames").get("dataframe").copy()
        valid_columns = self.data.get("frames").get("dataframe")
        valid_filters = ("==", "!=", ">=", "<=", "<", ">")
        column = input(" : column to filter on: ")
        if column not in valid_columns: return "not a valid column!"
        column_type = current_dataframe[column].dtype
        print(f" : column '{column}' is of type {column_type}")
        column_is = input(" : way to filter (==, !=, >=, <=, <, >): ")
        if column_is not in valid_filters: return "not a valid filter!"
        filter_value = input(" : enter value to filter by: ")

        if filter_value.isdigit():
            filter_value = int(filter_value)
        
        try:
            if column_is == "==":
                current_dataframe = current_dataframe[current_dataframe[column]==filter_value]
            elif column_is == "!=":
                current_dataframe = current_dataframe[current_dataframe[column]!=filter_value]
            elif column_is == ">=":
                current_dataframe = current_dataframe[current_dataframe[column]>=filter_value]
            elif column_is == "<=":
                current_dataframe = current_dataframe[current_dataframe[column]<=filter_value]
            elif column_is == ">":
                current_dataframe = current_dataframe[current_dataframe[column]>filter_value]
            elif column_is == "<":
                current_dataframe = current_dataframe[current_dataframe[column]<filter_value]
        except Exception as error:
            return f"an error was encounter ; {error}"

        self._bubble(current_dataframe.copy())
        del current_dataframe

        return "filter successfully applied!"

    def rollback(self):
        v2 = self.data.get("frames").get("_dataframe")
        v3 = self.data.get("frames").get("__dataframe")

        self.set_frames(
            v2,
            v3,
            None
        )

    def drop(self):
        if not self.is_loaded(): return "no data loaded!"
        valid_columns = self.data.get("frames").get("dataframe")
        drop_columns = input(" : enter columns to drop (, separated): ")
        drop_columns = [col.strip() for col in drop_columns.split(",")]
        current_dataframe = self.data.get("frames").get("dataframe").copy()
        for col in drop_columns:
            if col in valid_columns:
                current_dataframe = current_dataframe.drop(columns=[col])
            else:
                print(f" : '{col}' not a column in the dataframe")
        
        self._bubble(current_dataframe.copy())
        del current_dataframe

        return "drop successfully applied!"

    def shell(self):
        while (query := input(":: ")) not in ["Q", "q", "quit"]:
            _query = query.strip().lower()
            _queries = [_.strip() for _ in _query.split("&")]
            for _q in _queries:
                if _q in self.commands.keys():
                    try:
                        response = self.commands[_q]()
                    except KeyboardInterrupt:
                        response = "command interrupted"
                    if response:
                        print(f" : {response}")
                else:
                    print(f" : '{_q}' is not a valid command")
        else:
            print(" : exiting")
        

if __name__ == "__main__":
    df = VersionControlledDataframe()
    df.shell()