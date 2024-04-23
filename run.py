from lark import Lark, Transformer, Tree, Token
import os, sys
from berkeleydb import db
import pickle
import ErrorMessage as em


with open('grammar.lark') as file:
    sql_parser = Lark(file.read(),start="command",lexer="basic")

id = '2020-13624'
prompt = "DB_"+id+'> '

    
#각 query에 해당하는 것에 대한 request를 받았다는 출력을 하는 메소드


class MyTransformer(Transformer):
    def __init__(self, prompt):
        super().__init__()
        self.prompt = prompt
        self.printer = em.PrintError(prompt)

        # Create directory if it does not exist
        if not os.path.exists('DB'):
            os.makedirs('DB')
        # Open BerkeleyDB databases for schema and data
        self.my_schema_db = db.DB()
        self.my_schema_db.open("DB/mySchemaDB.db", db.DB_HASH, db.DB_CREATE)
        self.my_data_db = db.DB()
        self.my_data_db.open("DB/myDataDB.db", db.DB_HASH, db.DB_CREATE)
        self.schema_cursor = self.my_schema_db.cursor()
        self.data_cursor = self.my_data_db.cursor()


    # Create table
    def create_table_query(self, items):
        
        table_name = items[2].children[0].lower() #table name
        column_definition_iter = items[3].find_data("column_definition")
        primary_key_constraint_iter = items[3].find_data("primary_key_constraint")
        foreign_key_constraint_iter = items[3].find_data("referential_constraint")
        
        column_name_list = []
        column_constraint_list = []
        primary_key_list= []
        foreign_key_list = []
        referenced_table_and_columns_list = []
        # Initialize primary key count for checking duplicate primary key definitions
        primary_key_count = 0
        
        #get primary key
        for primary_key_constraint in primary_key_constraint_iter:
            if primary_key_constraint.children and len(primary_key_constraint.children) > 2:
                for column_name in primary_key_constraint.children[2].children:
                    if column_name and isinstance(column_name, Tree) and column_name.data == "column_name":
                        primary_key_list.append(column_name.children[0].value.lower())
                    elif column_name and isinstance(column_name, Token) and column_name.type == "IDENTIFIER":
                        primary_key_list.append(column_name.value.lower())
                primary_key_count += 1

        # get foreign key and referenced table and columns
        for foreign_key_constraint in foreign_key_constraint_iter:
            if foreign_key_constraint.children and len(foreign_key_constraint.children) > 2:
                foreign_key_columns = []
                for column_name in foreign_key_constraint.children[2].children:
                    if column_name and isinstance(column_name, Tree) and column_name.data == "column_name":
                        foreign_key_columns.append(column_name.children[0].value.lower())
                    elif column_name and isinstance(column_name, Token) and column_name.type == "IDENTIFIER":
                        foreign_key_columns.append(column_name.value.lower())

                if foreign_key_columns:  # only proceed if foreign key columns were found
                    referenced_table = None
                    referenced_columns = []
                    for child in foreign_key_constraint.children:
                        if isinstance(child, Token) and child.type == "REFERENCES":
                            referenced_table = foreign_key_constraint.children[foreign_key_constraint.children.index(child) + 1].children[0].value.lower()
                            referenced_columns_iter = (column for column in foreign_key_constraint.children[foreign_key_constraint.children.index(child) + 2].children if isinstance(column, Tree) and column.data == "column_name")
                            referenced_columns = [column.children[0].value.lower() for column in referenced_columns_iter] if referenced_columns_iter else []

                    if referenced_table and referenced_columns:  # only proceed if referenced table and columns were found
                        referenced_table_and_columns_list.append((tuple(foreign_key_columns), referenced_table, tuple(referenced_columns)))
        
        for column_definition in column_definition_iter:
            if column_definition.children and len(column_definition.children) > 0:
                column_name = column_definition.children[0].children[0].lower() if column_definition.children[0].children else None
                column_type = column_definition.children[1].children[0].lower() if column_definition.children[1].children else None

                # If there are parentheses, include them and their contents
                if column_definition.children[1].children and len(column_definition.children[1].children) > 1:
                    column_type += column_definition.children[1].children[1] + column_definition.children[1].children[2] + column_definition.children[1].children[3] if column_definition.children[1].children else None

                # Check if null is allowed
                if column_name in primary_key_list:
                    column_null = 'N'
                elif column_definition.children[2] is not None:
                    column_null = 'N'
                else:
                    column_null = 'Y'

                # check if there is a reference
                column_reference = ''
                for foreign_key, referenced_table, referenced_columns in referenced_table_and_columns_list:
                    if foreign_key[0] not in foreign_key_list:
                        foreign_key_list.append(foreign_key[0])
                    if column_name in foreign_key:
                        foreign_key_list.append(column_name)
                        column_reference = (referenced_table, referenced_columns)
                        break
                
                # Check if the column is a primary key, foreign key, or both
                if column_name in primary_key_list and column_name in foreign_key_list:
                    column_key = 'PRI/FOR'
                elif column_name in primary_key_list:
                    column_key = 'PRI'
                elif column_name in foreign_key_list:
                    column_key = 'FOR'
                else:
                    column_key = ''

            if column_name is not None and column_type is not None and column_null is not None and column_key is not None:
                column_name_list.append(column_name)
                column_constraint_list.append((column_type, column_null, column_key, column_reference))   
        
        # Check if all foreign keys exist in the column names
        for foreign_key in foreign_key_list:
            if foreign_key not in column_name_list:
                self.printer.non_existing_column_def_error(foreign_key)
                return
        
        # Check for table existence errors
        result = self.schema_cursor.first()
        while result is not None:
            key, _ = result
            if key.decode() == table_name:
                self.printer.table_existence_error()
                self.schema_cursor.close()
                return
            result = self.schema_cursor.next()               
                
        # Check for duplicate column names
        if len(set(column_name_list)) != len(column_name_list):
            self.printer.duplicate_column_def_error()
            return

        # Check for duplicate primary key definitions
        if primary_key_count > 1:
            self.printer.duplicate_primary_key_def_error()
            return
        
        # Check for self-referencing errors
        for column_name,(column_type, column_null, column_key, column_reference) in zip(column_name_list, column_constraint_list):

            if column_key == 'FOR' or column_key == 'PRI/FOR':
                reference_table, reference_column = column_reference
                if reference_table == table_name and reference_column == column_name:
                    self.printer.non_existing_column_def_error()
                    return
        
        
        # Check for reference type errors
        # Check if char_id is in foreign_key_list
        #print("char_id" in foreign_key_list)  # Add this line
        for column_name, (column_type, column_null, column_key, column_reference) in zip(column_name_list, column_constraint_list):
            if column_key == 'FOR' or column_key == 'PRI/FOR':
                referenced_table, referenced_columns = column_reference
                
                if referenced_table is None or referenced_columns is None:
                    self.printer.reference_column_existence_error()
                    return

                # Check if the referenced table exists
                referenced_table_schema = self.my_schema_db.get(referenced_table.encode())
                if referenced_table_schema is None:
                    self.printer.reference_table_existence_error()
                    return
                
                # Check if the column exists in the current table
                
                if column_name not in column_name_list:
                    self.printer.non_existing_column_def_error(column_name)
                    return
                    
                # Get the type of the referenced columns
                referenced_table_schema = pickle.loads(referenced_table_schema)
                for column_name, referenced_column in zip(column_name_list, referenced_columns):
                    referenced_column_type = None
                    for ref_column_name, (ref_column_type, _, _, _) in referenced_table_schema:
                        if ref_column_name == referenced_column:
                            referenced_column_type = ref_column_type
                            break
                    
                    # If the referenced column does not exist, print an error message
                    if referenced_column_type is None:
                        self.printer.reference_column_existence_error()
                        return

                    # Update column_type only if there are multiple referenced_columns
                    if len(referenced_columns) > 1:
                        column_type = [column_type for column_name, (column_type, _, _, _) in zip(column_name_list, column_constraint_list) if column_name == referenced_column][0]
                    
                    # Check if the types match
                    if column_type != referenced_column_type:   
                        self.printer.reference_type_error()
                        return
                    
        # Check for reference table existence errors and reference column existence errors and reference non-primary key errors
        if column_key == 'FOR' or column_key == 'PRI/FOR':
            referenced_table, referenced_column = column_reference

            # Check if the referenced table exists
            referenced_table_bytes = self.my_schema_db.get(referenced_table.encode())
            if referenced_table_bytes is None:
                self.printer.reference_table_existence_error()
                return

            # Get the schema of the referenced table
            referenced_table_schema = pickle.loads(referenced_table_bytes)

            
        # Check for non-existing column definition errors
        for column_name in primary_key_list + foreign_key_list:
            if column_name not in column_name_list:
                self.printer.non_existing_column_def_error(column_name)
                return


            # Check for reference type errors and reference table existence errors
        for column_name, (column_type, column_null, column_key, column_reference) in zip(column_name_list, column_constraint_list):
            if column_key == 'FOR' or column_key == 'PRI/FOR':
                referenced_table, referenced_columns = column_reference
                
                if referenced_table is None or referenced_columns is None:
                    self.printer.reference_column_existence_error()
                    return
                # Initialize the dictionary of referenced columns
                referenced_columns_dict = {}

                # Check for reference type errors and reference table existence errors
                for column_name, (column_type, column_null, column_key, column_reference) in zip(column_name_list, column_constraint_list):
                    if column_key == 'FOR' or column_key == 'PRI/FOR':
                        referenced_table, referenced_column = column_reference
                        if referenced_table is None or referenced_column is None:
                            self.printer.reference_column_existence_error()
                            return

                        # Add the referenced columns of each foreign key to the dictionary
                        if referenced_table not in referenced_columns_dict:
                            referenced_columns_dict[referenced_table] = []
                        referenced_columns_dict[referenced_table].append(referenced_column)

                for referenced_table, referenced_columns in referenced_columns_dict.items():
                    # Get the schema of the referenced table
                    referenced_table_schema = pickle.loads(self.my_schema_db.get(referenced_table.encode()))
                    # Get the column names of the referenced table
                    referenced_table_columns = [column_name for column_name, _ in referenced_table_schema]

                    # Flatten the referenced_columns list
                    flat_referenced_columns = [column for sublist in referenced_columns for column in sublist]

                    # Check if the referenced column exists in the referenced table
                    if not all(item in referenced_table_columns for item in flat_referenced_columns):
                        self.printer.reference_column_existence_error()
                        return


                # Add the referenced columns of each foreign key to the dictionary
                for column_name, (column_type, column_null, column_key, column_reference) in zip(column_name_list, column_constraint_list):
                    if column_key == 'FOR' or column_key == 'PRI/FOR':
                        referenced_table, referenced_columns = column_reference
                        if referenced_table not in referenced_columns_dict:
                            
                            referenced_columns_dict[referenced_table] = []
                        referenced_columns_dict[referenced_table].append(referenced_columns)

                # Check if all primary key columns are referenced for each table
                for referenced_table, referenced_columns in referenced_columns_dict.items():       
                    # Get the schema of the referenced table
                    referenced_table_schema = pickle.loads(self.my_schema_db.get(referenced_table.encode()))
                    # Get the primary key columns of the referenced table
                    
                    primary_key_columns = [column_name for column_name, (_, _, key, _) in referenced_table_schema if key == 'PRI' or key == 'PRI/FOR']
                
                    # Flatten the list of referenced_columns
                    referenced_columns = [item for sublist in referenced_columns for item in sublist]

                    # Check if all primary key columns are referenced
                    if set(referenced_columns) != set(primary_key_columns):
                    
                        self.printer.reference_non_primary_key_error()
                        return


        # Check for char length errors
        for column_name, (column_type, column_null, column_key, column_reference) in zip(column_name_list, column_constraint_list):
            if column_type.startswith("char"):
                length = int(column_type[5:-1])
                if length < 1:
                    self.printer.char_length_error()
                    return

        # Save table information and print success message
        # $SAVE_TABLE_INFO$
         # Create table (BerkeleyDB database)
        # zip makes tuple of column name and column type/ ex) ('column1', ('int', 'not null', 'primary_key', ''))
        schema = list(zip(column_name_list, column_constraint_list))
        self.my_schema_db.put(table_name.encode(), pickle.dumps(schema))
        self.printer.create_table_success(table_name)

        
        
    def drop_table_query(self, items):
        table_name  = items[2].children[0].lower()
        # Check if table exists
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.no_such_table()
            return

        # Check if table is referenced by another table
        for key in self.my_schema_db.keys():
            schema_data = self.my_schema_db.get(key)
            schema = pickle.loads(schema_data)
            for column in schema:
                if column[1][3] and len(column[1][3]) > 0:
                    if column[1][3][0] == table_name: 
                        self.printer.drop_referenced_table_error(table_name)
                        return

        # Drop table
        self.my_schema_db.delete(table_name.encode())

        if self.my_data_db.exists(table_name.encode()):
            self.my_data_db.delete(table_name.encode())
        self.printer.drop_success(table_name)
        


    def select_query(self, items):
        """
        Executes a SELECT query on the specified table.

        Args:
            items (list): A list of items representing the parsed query.

        Returns:
            None

        Raises:
            None

        """
        table_name = items[2].children[0].children[1].children[0].children[0].children[0].value.lower()
        # Check if table exists
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.select_table_existence_error(table_name)
            return

        # SELECT *
        if isinstance(items[1], Tree) and items[1].data == "select_list" and len(items[1].children) == 0:
            try:
                data = self.my_data_db.get(table_name.encode())
                unpickled_data = pickle.loads(data) if data is not None else []

                # Get schema
                schema_data = self.my_schema_db.get(table_name.encode())
                schema = pickle.loads(schema_data)

                # Extract column names from schema
                column_names = [column[0] for column in schema]
                # Calculate max length for each column
                max_lengths = [max(len(str(row[column].value)) if isinstance(row[column], Token) else len(str(row[column])), len(column)) for row in unpickled_data for column in column_names] if unpickled_data else [len(column) for column in column_names]
                for column, max_length in zip(column_names, max_lengths):
                    print('+' + '-' * (max_length + 2), end='')
                print('+')
                for column, max_length in zip(column_names, max_lengths):
                    print('| ' + column.upper().ljust(max_length) + ' ', end='')
                print('|')
                for column, max_length in zip(column_names, max_lengths):
                    print('+' + '-' * (max_length + 2), end='')
                print('+')
                for row in unpickled_data:
                    for column, max_length in zip(column_names, max_lengths):
                        value = str(row[column]) if row[column] is not None else 'NULL'  # Get the value directly from row[column]
                        if isinstance(row[column], str):  # if the value is a string, remove the quotes
                            value = value.strip('\'"')
                        print('| ' + value.ljust(max_length) + ' ', end='')
                    print('|')
                for column, max_length in zip(column_names, max_lengths):
                    print('+' + '-' * (max_length + 2), end='')
                print('+')
            except pickle.UnpicklingError:
                print("Data is not pickle data")

        # SELECT column_name for project 1-3
        elif isinstance(items[1], Tree) and items[1].data == "select_list":
            column_names = [child.children[1].children[0].value.lower() for child in items[1].children]
            print('SELECT', ', '.join(column_names), 'FROM', table_name)
            
    def insert_query(self, items):
        # Parse the command
        table_name = items[2].children[0] if isinstance(items[2], Tree) else str(items[2])
        values = []
        for item in items[5].children:
            if isinstance(item, Tree):
                token = item.children[0]
                if isinstance(token, Token) and token.type == 'NULL':
                    values.append(None)
                else:
                    values.append(str(token))
            elif isinstance(item, Token) and item.type in ['INT', 'STR']:
                values.append(str(item))

        # Check if the table exists
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.no_such_table()
            return

        # Get the schema of the table
        table_schema = pickle.loads(self.my_schema_db.get(table_name.encode()))
        schema_column_names = [column_name for column_name, _ in table_schema]

        # Check if the number of values matches the number of columns
        if len(values) != len(table_schema):
            self.printer.insert_value_error()
            return
        # Get column names and values from query
        # when Explicit column names are provided
        if items[3] != None:
            query_column_names = [child.children[0].value.lower() for child in items[3].children if isinstance(child, Tree)]
            
            # Check if all column names exist in the schema
            for column_name in query_column_names:
                if column_name not in schema_column_names:
                    self.printer.insert_value_error()
                    return

            # Check if the values are valid
            for i, (column_name, value) in enumerate(zip(query_column_names, values)):
                # Find the corresponding column schema
                column_schema = next((column for column in table_schema if column[0] == column_name), None)
                if column_schema is None:
                    self.printer.insert_value_error()
                    return
              
                column_type = column_schema[1][0]
                column_null = column_schema[1][1]

                # If the value is NULL
                if column_null == 'N' and value is None:
                    self.printer.insert_value_error()
                    return
                if column_type.startswith('char'):
                    if value is None:
                        values[i] = "null"
                    elif not isinstance(value, str):
                        self.printer.insert_value_error()
                        return
                    if isinstance(value, str):
                        if len(value) > int(column_type.split('(')[1][:-1]):
                            # Update the value to its truncated version
                            value = value[:int(column_type.split('(')[1][:-1])+1]
                            # Update the value in the values list
                            values[i] = value
                    if column_type == 'int' and not str(value).isdigit():
                        self.printer.insert_value_error()
                        return
            # Sort query_values according to the order of schema_column_names
            values = [value for _, value in sorted(zip(query_column_names, values), key=lambda pair: schema_column_names.index(pair[0]))]

            # Insert the values into the table
            table_data = pickle.loads(self.my_data_db.get(table_name.encode(), pickle.dumps([])))

            # Create row_data according to the order of schema_column_names
            row_data = {column_name: None for column_name in schema_column_names}
            for column_name in schema_column_names:
                if column_name in query_column_names:
                    index = schema_column_names.index(column_name)
                    row_data[column_name] = values[index]
            # Add row_data to table_data
            table_data = pickle.loads(self.my_data_db.get(table_name.encode(), pickle.dumps([])))
            table_data.append(row_data)

            # Update the table in the database
            self.my_data_db.put(table_name.encode(), pickle.dumps(table_data))

            # Print success message
            self.printer.insert_result()
            return

        # Check if the values are valid
        for i, (column_name, (column_type, column_null, column_key, column_reference)) in enumerate(table_schema):
            value = values[i]
            if column_null == 'N' and value is None:
                self.printer.insert_value_error()
                return
            if column_type.startswith('char'):
                if value is None:
                    values[i] = "null"
                elif not isinstance(value, str):
                    self.printer.insert_value_error()
                    return
                if isinstance(value, str):
                    if len(value) > int(column_type.split('(')[1][:-1]):
                        # Update the value to its truncated version
                        value = value[:int(column_type.split('(')[1][:-1])+1]
                        # Update the value in the values list
                        values[i] = value
            if column_type == 'int' and not str(value).isdigit():
                self.printer.insert_value_error()
                return
            
            
        # Prepare the row data as a dictionary
        row_data = {column_name: value for (column_name, _), value in zip(table_schema, values)}

        # Insert the values into the table
        table_data = pickle.loads(self.my_data_db.get(table_name.encode(), pickle.dumps([])))
        table_data.append(row_data)
        self.my_data_db.put(table_name.encode(), pickle.dumps(table_data))

        # Print success message
        self.printer.insert_result()


    def delete_query(self, items):
        print(prompt+"'DELETE' requested")
    def update_query(self, items):
        print(prompt+"'UPDATE' requested")


    def explain_query(self,items):
        # Get the table name from the query
        table_name = items[1].children[0].lower()
        
        # Check if the table exists in the schema database
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.no_such_table()
            return

        # Get the schema of the table from the schema database
        table_schema = pickle.loads(self.my_schema_db.get(table_name.encode()))

        # Print the table name
        print('-' * 65)
        print(f'table_name [{table_name}]')
        print('{:<15} {:<10} {:<5} {:<10}'.format('column_name', 'type', 'null', 'key'))

        # Print the schema of the table
        for column_name, (column_type, column_null, column_key, column_reference) in table_schema:
            print('{:<15} {:<10} {:<5} {:<10}'.format(column_name, column_type, column_null, column_key))

        print('-' * 65)


    def describe_query(self,items):
        table_name = items[1].children[0].lower()
        # Check if the table exists
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.no_such_table()
            return

        # Get the schema of the table
        table_schema = pickle.loads(self.my_schema_db.get(table_name.encode()))

        # Print the table name
        print('-' * 65)
        print(f'table_name [{table_name}]')
        print('{:<15} {:<10} {:<5} {:<10}'.format('column_name', 'type', 'null', 'key'))

        # Print the schema
        for column_name, (column_type, column_null, column_key, column_reference) in table_schema:
             print('{:<15} {:<10} {:<5} {:<10}'.format(column_name, column_type, column_null, column_key))

        print('-' * 65)

    def desc_query(self,items):
        table_name = items[1].children[0].lower()
        # Check if the table exists
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.no_such_table()
            return

        # Get the schema of the table
        table_schema = pickle.loads(self.my_schema_db.get(table_name.encode()))

        # Print the table name
        print('-' * 65)
        print(f'table_name [{table_name}]')
        print('{:<15} {:<10} {:<5} {:<10}'.format('column_name', 'type', 'null', 'key'))

        # Print the schema
        for column_name, (column_type, column_null, column_key, column_reference) in table_schema:
             print('{:<15} {:<10} {:<5} {:<10}'.format(column_name, column_type, column_null, column_key))

        print('-' * 65)

    def show_tables_query(self,items):
        # Get all the table names
        table_names = [key.decode() for key in self.my_schema_db.keys()]

        # Print the table names
        print('-' * 24)
        for table_name in table_names:
            print(table_name)
        print('-' * 24)


    def EXIT(self,items):
        """
        Closes the database connections and exits the program.

        Args:
            items (list): A list of items representing the parsed query.

        Returns:
            None

        Raises:
            None
        """
        self.schema_cursor.close()
        self.data_cursor.close()
        self.my_schema_db.close()
        self.my_data_db.close()
        sys.stdout.flush()
        os._exit(0)

#input_str을 빈 문자열로 초기화한다.
input_str="" 
#input이 ';'으로 끝나지 않을 경우 flag를 True로 세팅한다.
#이에 따라 prompt를 출력하지 않도록 한다.
flag = False
while(True):
    #input으로 ';'을 받지 못한 경우나 ';'으로 끝나지 않은 경우
    #input_str에 tmp를 계속 이어붙여야 한다.
    if(flag): tmp = input()
    try:
        tmp = input(prompt)
    except EOFError:
        print("EOF detected. Exiting the program.")
        break  # or sys.exit(), depending on your needs
    input_str += tmp
    #';'으로 끝나지 않은 경우
    if(not input_str.endswith(';') or input_str == ''):
        flag=True
        continue
    # ';'으로 끝났을 경우
    if input_str.endswith(';'):
        # 받은 input_str을 ;에 대해 split한다.
        query_list = [s.strip() for s in input_str.split(';') if s.strip()]
        if all(q == '' for q in query_list):
            # 입력이 ';'만 있는 경우
            print(prompt+"Syntax error")
            input_str = ""
            flag = False
            continue
        for q in query_list:
            # query_list element에 다시 ;을 붙여주고 transform에 넘겨준다.
            try:
                output = sql_parser.parse(q+';')
                MyTransformer(prompt).transform(output)
            except Exception as e:
                # 문법상 오류가 있을 경우 Syntax error를 출력한다.
                print(prompt+"Syntax error",e)
                input_str = ""
                flag=False
                break

        # 그 후 다시 input_str을 초기화해준다.
            input_str=''
            flag=False
                     

                 