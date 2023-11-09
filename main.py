import re
import os
import pandas as pd

class IterativeShell:

    def __init__(self):
        self.commands = {
            "hello": self.hello,
            "data": self.data,
            "ik": self.ik,
            "view": self.view,
            "filter": self.filter_where,
            "rollback": self.rollback,
            "drop": self.drop,
            "score": self.score,
            "columns": self.columns,
        }

        self.data = {
            "ik": {
                "clover": "clv",
                "clvr": "clv",
                "tck": "telecheck",
                "ecommerce": "ecom",
                "chargebacks": "chargeback",
                "qsr": "quick_service",
                "act": "mag"
            },
            "frames": {
                "base": None,
                "dataframe": None,
                "_dataframe": None,
                "__dataframe": None,
            },
            "memory_paths": {
                "dataframe": "./memory/a.pkl",
                "_dataframe": "./memory/aa.pkl",
                "__dataframe": "./memory/aaa.pkl",
            },
            "has_updated": False,
        }
    
    def hello(self):
        return "Hello World!"

    def _save_frames_to_memory(self):
        if self.data.get("has_updated"):
            print(" : saving progress...")

            frames = list(self.data.get("frames"))[1:]
            base_mem_path = "memory"
            memory_paths = list(self.data.get("memory_paths").values())

            for frame_idx, frame in enumerate(frames):
                if (tmp := self.data.get("frames").get(frame)) is not None:
                    tmp.to_pickle(memory_paths[frame_idx])
                else:
                    if os.path.exists(memory_paths[frame_idx]):
                        os.remove(memory_paths[frame_idx])

    def _check_and_load(self):
        base_mem_path = "./memory"
        memory_files = os.listdir(base_mem_path)
        if len(memory_files) != 0:
            frame_idxs = list(self.data.get("frames"))[1:]
            print(" : loading previous state...")
            for mem_idx, mem_file in enumerate(memory_files):
                self.data["frames"][frame_idxs[mem_idx]] = pd.read_pickle(f"{base_mem_path}/{mem_file}")
            print(" : previous state successfully loaded!")

    def set_frames(self, df1, df2, df3):
        self.data.get("frames")["dataframe"] = df1
        self.data.get("frames")["_dataframe"] = df2
        self.data.get("frames")["__dataframe"] = df3

    def is_loaded(self, *args):
        return self.data.get("frames").get("dataframe") is not None

    def view(self, *args):
        if not self.is_loaded(): return "no data loaded!"
        if args and args[0].strip() != '':
            filter_cols = args[0]
            if filter_cols[0] != "[" or filter_cols[-1] != "]":
                return "arg of 'view' must look like '[col1, col2, col3, ...]' to get a filtered view"
            valid_columns = self.data.get("frames").get("dataframe").columns.values.tolist()
            filter_cols = [_.strip() for _ in filter_cols[1:-1].split(",")]
            filter_cols = [col for col in filter_cols if col in valid_columns]
            print(self.data.get("frames").get("dataframe").head()[filter_cols])
        else:
            print(self.data.get("frames").get("dataframe").head())

    def _bubble(self, bubbleframe):
        if not self.data.get("has_updated"):
            self.data["has_updated"] = True

        v1 = self.data.get("frames").get("dataframe")
        v2 = self.data.get("frames").get("_dataframe")

        self.set_frames(
            bubbleframe,
            v1,
            v2
        )

    def filter_where(self, *args):
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

        if (filter_value.isdigit()) or (filter_value[0] == '-' and filter_value[1:].isdigit()):
            filter_value = int(filter_value)
        if filter_value in ["True", "False"]:
            filter_value = filter_value == "True"
        
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

    def rollback(self, *args):
        if not self.is_loaded(): return "no data loaded, nothing to rollback!"

        if not self.data.get("has_updated"):
            self.data["has_updated"] = True

        v2 = self.data.get("frames").get("_dataframe")
        v3 = self.data.get("frames").get("__dataframe")

        self.set_frames(
            v2,
            v3,
            None
        )

        return "rollback successful!"

    def drop(self, *args):
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

    def columns(self, *args):
        if not self.is_loaded(): return "no data loaded!"
        return ", ".join(self.data.get("frames").get("dataframe").columns.values.tolist())

    def enter(self):
        self._check_and_load()

        try:
            while (query := input(":: ")) not in ["Q", "q", "quit"]:
                _query = query.strip().lower()
                _queries = [_.strip() for _ in _query.split("&")]
                for _q in _queries:
                    _q_parts = _q.split(" ")
                    if _q_parts[0] in self.commands.keys():
                        try:
                            response = self.commands[_q_parts[0]](" ".join(_q_parts[1:]))
                        except KeyboardInterrupt:
                            response = "command interrupted"
                        except Exception as error:
                            response = f"command '{_q_parts[0]}' failed with error: '{error}'"

                        if response:
                            print(f" : {response}")
                    else:
                        print(f" : '{_q}' is not a valid command")
            else:
                self._save_frames_to_memory()
                print(" : exiting")
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
            self._save_frames_to_memory()
            print(" : exiting")


if __name__ == "__main__":
    shell = IterativeShell()
    shell.enter()