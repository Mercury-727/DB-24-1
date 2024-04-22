class PrintError:
    def __init__(self, prompt):
        self.prompt = prompt

    def syntax_error(self):
        print(self.prompt + "Syntax error")

    def create_table_success(self, table_name):
        print(self.prompt + f"'{table_name}' table is created")

    def duplicate_column_def_error(self):
        print(self.prompt + "Create table has failed: column definition is duplicated")

    def duplicate_primary_key_def_error(self):
        print(self.prompt + "Create table has failed: primary key definition is duplicated")

    def reference_type_error(self):
        print(self.prompt + "Create table has failed: foreign key references wrong type")

    def reference_non_primary_key_error(self):
        print(self.prompt + "Create table has failed: foreign key references non primary key column")

    def reference_column_existence_error(self):
        print(self.prompt + "Create table has failed: foreign key references non existing column")

    def reference_table_existence_error(self):
        print(self.prompt + "Create table has failed: foreign key references non existing table")

    def non_existing_column_def_error(self, col_name):
        print(self.prompt + f"Create table has failed: '{col_name}' does not exist in column definition")

    def table_existence_error(self):
        print(self.prompt + "Create table has failed: table with the same name already exists")

    def char_length_error(self):
        print(self.prompt + "Char length should be over 0")

    def drop_success(self, table_name):
        print(self.prompt + f"'{table_name}' table is dropped")

    def no_such_table(self):
        print(self.prompt + "No such table")

    def drop_referenced_table_error(self, table_name):
        print(self.prompt + f"Drop table has failed: '{table_name}' is referenced by other table")

    def insert_result(self):
        print(self.prompt + "The row is inserted")

    def insert_value_error(self):
        print(self.prompt + "Insertion has failed: type mismatch")

    def select_table_existence_error(self, table_name):
        print(self.prompt + f"Selection has failed: '{table_name}' does not exist")