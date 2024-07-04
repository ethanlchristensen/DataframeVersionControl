import os
import pandas as pd

class IterativeShell:

    def __init__(self):
        self.commands = {
            "hello":    {'command': self.hello, 'info': 'test command'},
            "data":     {'command': self.data, 'info': 'command to load in the data'},
            "view":     {'command': self.view, 'info': 'command to view the current dataframe'},
            "filter":   {'command': self.filter_where, 'info': 'command to filter the dataframe'},
            "rollback": {'command': self.rollback, 'info': 'command to revert back to older version of the dataframe'},
            "drop":     {'command': self.drop, 'info': 'command to drop columns from the dataframe'},
            "columns":  {'command': self.columns, 'info': 'command to see the columns in the dataframe'},
            "base":     {'command': self.base_load, 'info': 'command to load the orignal df into the base store (for scoring), called when preious state is loaded'},
            "help":     {'command': self.help, 'info': 'command to see info about other commands'},
            "csv":      {'command': self.csv, 'info': 'command to save the current dataframe to a csv'}
        }

        self.data = {
            "views": {
                "small-view": ["NAME", "Species", "GENDER"],
                "c": ["NAME"],
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

        view_df = self.data.get("frames").get("dataframe")

        if args and args[0].strip() != '':
            command_args = args[0]
            if "|" in command_args:
                columns = command_args.split("|")[0]
                filters = command_args.split("|")[1]
            else:
                columns = command_args
                filters = None

            valid_columns = view_df.columns.values.tolist()

            if columns[0] == "[" and columns[-1] == "]":
                columns = [_.strip() for _ in columns[1:-1].split(",")]
                columns = [col for col in columns if col in valid_columns]
            elif columns.startswith("load"):
                try:
                    _view_tag = columns.split(":")[1]
                except:
                    return "use 'view load:<view-name>' to load an existing view"
                
                columns = self.data.get("views").get(_view_tag)

                if not columns:
                    return f"could not load view called '{_view_tag}'"
            else:
                return "arg of 'view' must look like '[col1, col2, col3, ...]' to get a filtered view"

            if filters:
                filters = [_.strip() for _ in filters.split(",")]
                for filter in filters:
                    _filter = [_.strip() for _ in filter.split(" ") if _ != ""]

                    if len(_filter) != 3:
                        print(f" : filter '{filter}' failed")
                        continue

                    view_df = self._filter(view_df, _filter[0], _filter[1], _filter[2])
                    
            print(view_df.head()[columns])
        else:
            print(view_df.head())
        
        print(f" : count of rows: {len(view_df)}")

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
    
    def _filter(self, in_df, column, column_is, filter_value):
        out_df = in_df.copy()

        if (filter_value.isdigit()) or (filter_value[0] == '-' and filter_value[1:].isdigit()):
            filter_value = int(filter_value)
        if filter_value in ["True", "False"]:
            filter_value = filter_value == "True"
        
        try:
            if column_is == "==":
                out_df = out_df[out_df[column]==filter_value]
            elif column_is == "!=":
                out_df = out_df[out_df[column]!=filter_value]
            elif column_is == ">=":
                out_df = out_df[out_df[column]>=filter_value]
            elif column_is == "<=":
                out_df = out_df[out_df[column]<=filter_value]
            elif column_is == ">":
                out_df = out_df[out_df[column]>filter_value]
            elif column_is == "<":
                out_df = out_df[out_df[column]<filter_value]
        except Exception as error:
            print(f" : filter not applied, error encountered: {error}")
            return in_df

        return out_df

    def filter_where(self, *args):
        if not self.is_loaded(): return "no data loaded!"
        current_dataframe = self.data.get("frames").get("dataframe").copy()

        previous_size = len(current_dataframe)

        valid_columns = self.data.get("frames").get("dataframe")
        valid_filters = ("==", "!=", ">=", "<=", "<", ">")
        column = input(" : column to filter on: ")
        if column not in valid_columns: return "not a valid column!"
        column_type = current_dataframe[column].dtype
        print(f" : column '{column}' is of type {column_type}")
        column_is = input(" : way to filter (==, !=, >=, <=, <, >): ")
        if column_is not in valid_filters: return "not a valid filter!"
        filter_value = input(" : enter value to filter by: ")

        current_dataframe = self._filter(current_dataframe, column, column_is, filter_value)

        new_size = len(current_dataframe)

        self._bubble(current_dataframe.copy())

        del current_dataframe

        print(f" : old size: {previous_size}\n : new size: {new_size}\n : % of original: {(new_size/previous_size)*100:.2f}%")

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

    def data(self, *args):
        if args[0] == '':
            return "must pass in file path to load data"
        
        df = pd.read_csv(args[0])

        self.data["frames"]["base"] = df.copy()
        self.data["frames"]["dataframe"] = df.copy()

        del df

        self.data["has_updated"] = True

        return "data loaded successfully!"
        
    def base_load(self, *args):
        if args[0] == '':
            return "must pass in file path to load data"
        
        df = pd.read_csv(args[0])

        self.data["frames"]["base"] = df.copy()

        del df

        return "data loaded successfully!"

    def columns(self, *args):
        if not self.is_loaded(): return "no data loaded!"
        return ", ".join(self.data.get("frames").get("dataframe").columns.values.tolist())

    def csv(self, *args):
        df_to_save = self.data.get('frames').get('dataframe')
        
        if args and args[0].strip() != '':
            columns = args[0]

            if columns[0] != "[" or columns[-1] != "]":
                return "arg of 'view' must look like '[col1, col2, col3, ...]' to get a filtered view"

            valid_columns = df_to_save.columns.values.tolist()
            columns = [_.strip() for _ in columns[1:-1].split(",")]
            columns = [col for col in columns if col in valid_columns]

            df_to_save[columns].to_csv(f'./output_csv/save_{len(os.listdir("./output_csv"))+1}.csv')
            
        else:
            if (input(f" : do you want to save all columns to a csV? ({len(df_to_save.columns)} total columns) (y/n)")) == 'y':
                df_to_save.to_csv(f'./output_csv/save_{len(os.listdir("./output_csv"))+1}.csv')
            else:
                return "no data saved to csv"
        
    def help(self, *args):
        for command, command_vals in self.commands.items():
            print(f"{command:<15s}: {command_vals['info']}")

    def enter(self):
        self._check_and_load()

        try:
            while (query := input(":: ")) not in ["Q", "q", "quit"]:
                _query = query.strip()
                _queries = [_.strip() for _ in _query.split("&")]
                for _q in _queries:
                    _q_parts = _q.split(" ")
                    _q_parts[0] = _q_parts[0].lower()
                    if _q_parts[0] in self.commands.keys():
                        try:
                            response = self.commands[_q_parts[0]]['command'](" ".join(_q_parts[1:]))
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