from lark import Lark, Transformer, Tree, Token
import os, sys
from berkeleydb import db
import pickle
import ErrorMessage as em
from datetime import datetime
import itertools

with open('grammar.lark') as file:
    sql_parser = Lark(file.read(),start="command",lexer="basic")

id = '2020-13624'
prompt = "DB_"+id+'> '

# Check if the date string is in the correct format
def is_valid_date_format(date_string, date_format):
    try:
        datetime.strptime(date_string, date_format)
        return True
    except ValueError:
        return False
date_format = '%Y-%m-%d'

# compare two operands with operator
def comparator(operand1, operator, operand2):
    if operand1.isdigit():
        operand1 = int(operand1)
    if operand2.isdigit():
        operand2 = int(operand2)
    if operator == '=' and operand1 != operand2:
        return False
    elif operator == '<' and operand1 >= operand2:
        return False
    elif operator == '>' and operand1 <= operand2:
        return False
    elif operator == '<=' and operand1 > operand2:
        return False
    elif operator == '>=' and operand1 < operand2:
        return False
    elif operator == '!=' and operand1 == operand2:
        return False
    elif operator == 'is' and operand1 != operand2:
        return False
    elif operator == 'is not' and operand1 == operand2:
        return False
    else:
        return True
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
        #Extract all the table names
        table_names_iter = items[2].children[0].find_data("table_name")
        table_names = [child.children[0].value.lower() for child in table_names_iter]
        
        # Check if table exists
        for table_name in table_names:
            if not self.my_schema_db.exists(table_name.encode()):
                self.printer.select_table_existence_error(table_name)
                return
            
        
        table_column_names = []  # Format [table_name, [column_name1, column_name2, ...]]
        column_to_tables = {}  # Dictionary to track which tables have which columns

        # Match the table_name and column_name
        for table_name in table_names:
            if not self.my_schema_db.exists(table_name.encode()):
                self.printer.select_table_existence_error(table_name)
                return
            # Get the schema of the table
            table_schema = pickle.loads(self.my_schema_db.get(table_name.encode()))
            schema_column_names = [column_name for column_name, _ in table_schema]
            table_column_names.append([table_name, schema_column_names])
            for column_name in schema_column_names:
                if column_name in column_to_tables:
                    column_to_tables[column_name].append(table_name)
                else:
                    column_to_tables[column_name] = [table_name]

        # Update the column names to be fully qualified if they appear in multiple tables
        for column_name, tables in column_to_tables.items():
            if len(tables) > 1:
                for table_name in tables:
                    for table_entry in table_column_names:
                        if table_entry[0] == table_name:
                            index = table_entry[1].index(column_name)
                            table_entry[1][index] = f"{table_name}.{column_name}"

        

        # Get Data for each table and do the cartesian product
        data = []
        empty_table_flag = False
        for table_name, column_names in table_column_names:
            # Load the original data
            
            original_data = self.my_data_db.get(table_name.encode())
            updated_data = []
            if original_data is None:
                #Make the empty dictionary accroding to schema
                empty_table_flag = True
                continue    
                
            else:
                original_data = pickle.loads(original_data) 
            
            # Update the column names in the data
            
            for row in original_data:
                updated_row = {}
                for column_name, value in row.items():
                    # If the column name is in the current table's column names, add the table name as a prefix
                    if column_name not in column_names:
                        updated_column_name = table_name + '.' + column_name
                    else:
                        updated_column_name = column_name
                    updated_row[updated_column_name] = value
                updated_data.append(updated_row)

            # Add the updated data to the list
            data.append(updated_data)
        
        # Calculate the cartesian product of the data from all tables
        cartesian_product = list(itertools.product(*data))

        # Convert the cartesian product (which is a list of tuples) to a list of dictionaries
        result_data = []
        for row in cartesian_product:
            result_row = {}
            for table_data in row:
                result_row.update(table_data)
            result_data.append(result_row)

        if empty_table_flag:
            result_data = [{}]
        
      #if items[2].children[1] is not None, there is where clause
        #if items[2].children[1] is not None, there is where clause
        if items[2].children[1] is not None:
            # boolean term = 1 && boolean factor = 2 -> AND
            # boolean factor = 2 -> OR
            # boolean term = 1 && boolean factor = 1 -> just 1 where clause   
           
            boolean_factor_iter = items[2].children[1].find_data("boolean_factor")
            boolean_term_iter = items[2].children[1].find_data("boolean_term")
            boolean_factor_cnt = 0
            boolean_term_cnt = 0

            where_clause_table_names1 = []
            where_clause_table_names2 = []
            where_clause_attributes1 = []
            where_clause_attributes2 = []
            where_clause_operators = []
            where_clause_values = []
            
            const_flags = []
            for _ in boolean_term_iter:
                boolean_term_cnt += 1
            for boolean_factor_term in boolean_factor_iter:
                const_flag =False
                # Extract the attribute, operator, and value from the where clause
                boolean_factor_cnt += 1
                where_clause_table_name_iter = boolean_factor_term.find_data("table_name")
                where_clause_attribute_iter = boolean_factor_term.find_data("column_name")
                where_clause_operator_iter = boolean_factor_term.find_data("comp_op")
                where_clause_value_iter = boolean_factor_term.find_data("comparable_value")
                where_clause_null_iter = boolean_factor_term.find_data("null_operation")

                where_clause_table_name1 = ''
                where_clause_table_name2 = ''
                for child in where_clause_table_name_iter:
                    if where_clause_table_name1 == '':
                        where_clause_table_name1 = child.children[0].value.lower()
                    else:
                        where_clause_table_name2 = child.children[0].value.lower()
                where_clause_table_names1.append(where_clause_table_name1)
                where_clause_table_names2.append(where_clause_table_name2)
                
                where_clause_value = ''
                
                where_clause_operators += [child.children[0].value.lower() for child in where_clause_operator_iter]
                
                for child in where_clause_null_iter:    
                   
                    where_clause_value = 'null'
                    if child.children[1] is None:
                        where_clause_operators += [child.children[0].value]
                    else:
                        where_clause_operators += [child.children[0].value + ' ' + child.children[1].value]
                
                where_clause_value_cnt = 0
                const1 = ''
                const2 = 'gf'
                for child in where_clause_value_iter:
                    where_clause_value_cnt += 1
                    where_clause_value = child.children[0].value
                    if const1 == '':
                        const1 = where_clause_value
                    else:
                        const2 = where_clause_value
                where_clause_values.append(where_clause_value)
                
                if where_clause_value_cnt == 2:
                    const_flag = True
                    #check if the constants have same valeu
                    if const1.startswith("'") or const1.startswith('"'):
                        const1 = const1[1:-1]
                    if const2.startswith("'") or const2.startswith('"'):
                        const2 = const2[1:-1]
                   
                    where_clause_attributes1.append(const1)
                    where_clause_attributes2.append('')
                    #where_clause_values.append(const2)
                    
                
                const_flags.append(const_flag)
                if(not const_flag):
                    where_clause_attributes = [child.children[0].value.lower() for child in where_clause_attribute_iter] 
                    
                    where_clause_attributes1.append(where_clause_attributes[0])
                    if len(where_clause_attributes) > 1:
                        where_clause_attributes2.append(where_clause_attributes[1])
                    else:
                        where_clause_attributes2.append('')

                
                

            
            
            schemas = [pickle.loads(self.my_schema_db.get(table_name.encode())) for table_name in table_names]
            # Check if the column names exist in the schema
            column_names_dict = {table_name: [column_name for column_name, _ in schema] for table_name, schema in zip(table_names, schemas)}

            for i in range(len(where_clause_attributes1)):
                if const_flags[i] == False:
                    if not any(where_clause_attributes1[i] in column_names for column_names in column_names_dict.values()):
                        self.printer.where_column_not_exist()
                        return
                        
            for column_name in where_clause_attributes2:
                if column_name != '':
                    if not any(column_name in column_names for column_names in column_names_dict.values()):
                        self.printer.where_column_not_exist()
                        return
            
            # Check if the column names are ambiguous
            for index in range(len(where_clause_attributes1)):
                if const_flags[index] == False:
                    if where_clause_table_names1[index] != '':
                        temp_table_name = where_clause_table_names1[index]
                        table_schema = self.my_schema_db.get(temp_table_name.encode())
                        if table_schema is not None:
                            schema = pickle.loads(table_schema)
                            column_names = [column_name for column_name, _ in schema]
                            if where_clause_attributes1[index] not in column_names:
                                self.printer.select_column_resolve_error(where_clause_attributes1[index])
                                return
                        else:
                            self.printer.where_table_not_specified()
                            return
                    else:
                        count = 0   
                        for column_names in column_names_dict.values():
                            if where_clause_attributes1[index] in column_names:
                                count += 1
                        if count > 1:    
                            self.printer.where_ambiguous_reference()
                            return
            
            # Processing where_clause_attributes2 for ambiguity and existence
            for index in range(len(where_clause_attributes2)):
                if where_clause_attributes2[index] != '':
                    if where_clause_table_names2[index] != '':
                        temp_table_name = where_clause_table_names2[index]
                        table_schema = self.my_schema_db.get(temp_table_name.encode())
                        if table_schema is not None:
                            schema = pickle.loads(table_schema)
                            column_names = [column_name for column_name, _ in schema]
                            if where_clause_attributes2[index] not in column_names:
                                self.printer.select_column_resolve_error(where_clause_attributes2[index])
                                return
                        else:
                            self.printer.no_such_table()
                            return
                    else:
                        count = 0
                        for column_names in column_names_dict.values():
                            if where_clause_attributes2[index] in column_names:
                                count += 1
                        if count > 1:
                            self.printer.where_ambiguous_reference()
                            return

            table_schemas = {table_name: schema for table_name, schema in zip(table_names, schemas)}
            
            for i in range(len(where_clause_attributes1)):
                if const_flags[i] == False:
                    table_name = None
                    for name, schema in table_schemas.items():
                        if where_clause_attributes1[i] in [column_name for column_name, _ in schema]:
                            table_name = name
                            break
                    if table_name is None:
                        self.printer.where_column_not_exist()
                        return
                    
                    if where_clause_table_names1[i] == '':
                        
                        for table_name, schema in table_schemas.items():
                            for column_name, (column_type, column_null, column_key, column_reference) in schema:
                                if column_name == where_clause_attributes1[i]:
                                    
                                    if where_clause_attributes2[i] != '':
                                        
                                        # Compare types between attributes1 and attributes2
                                        attr2_table_name = where_clause_table_names2[i] if where_clause_table_names2[i] else where_clause_table_names1[i]
                                        attr2_schema = table_schemas[attr2_table_name]
                                        for col_name, (col_type, _, _, _) in attr2_schema:
                                            if col_name == where_clause_attributes2[i]:
                                                if col_type != column_type:
                                                    self.printer.where_incomparable_error()
                                                    return
                                    else:
                                        if column_type.startswith('char'):
                                            
                                            if where_clause_values[i] is None:
                                                where_clause_values[i] = "null"
                                            elif not isinstance(where_clause_values[i], str):
                                                self.printer.where_incomparable_error()
                                                return
                                            elif not where_clause_values[i].startswith(("'", '"')) and where_clause_values[i] != '':
                                                if not where_clause_values[i] == 'null':
                                                    self.printer.where_incomparable_error()
                                                    return
                                                
                                            if isinstance(where_clause_values[i], str):
                                                if len(where_clause_values[i]) > int(column_type.split('(')[1][:-1]):
                                                    where_clause_values[i] = where_clause_values[i][:int(column_type.split('(')[1][:-1])+1]
                                        if column_type == 'int' and (not(str(where_clause_values[i]).isdigit()) and where_clause_values[i] != 'null'):                
                                            self.printer.where_incomparable_error()
                                            return
                                        if where_clause_values[i] == 'null' and where_clause_operators[i] not in ['is', 'is not']:
                                            
                                            self.printer.where_incomparable_error()
                                            return
                                        if where_clause_values[i] != 'null':
                                            if column_type.startswith('char') and where_clause_operators[i] not in ['=', '!=']:
                                                self.printer.where_incomparable_error()
                                                return
                                            elif column_type == 'int' and where_clause_operators[i] not in ['=', '!=', '<', '>', '<=', '>=']:
                                                self.printer.where_incomparable_error()
                                                return
                                            elif column_type == 'date' and where_clause_operators[i] not in ['=', '!=', '<', '>', '<=', '>=']:
                                                self.printer.where_incomparable_error()
                                                return
                    else:
                        for column_name, (column_type, column_null, column_key, column_reference) in table_schemas[where_clause_table_names1[i]]:
                            if column_name == where_clause_attributes1[i]:
                                
                                if where_clause_attributes2[i] != '':
                                        # Compare types between attributes1 and attributes2
                                        attr2_table_name = where_clause_table_names2[i] if where_clause_table_names2[i] else where_clause_table_names1[i]
                                        attr2_schema = table_schemas[attr2_table_name]
                                        
                                        for col_name, (col_type, _, _, _) in attr2_schema:
                                            if col_name == where_clause_attributes2[i]:
                                                
                                                if col_type != column_type:
                                                    
                                                    self.printer.where_incomparable_error()
                                                    return
                                else:
                                    if column_type.startswith('char'):
                                        if where_clause_values[i] is None:
                                            where_clause_values[i] = "null"
                                        elif not isinstance(where_clause_values[i], str):
                                            self.printer.where_incomparable_error()
                                            return
                                        
                                        elif (not where_clause_values[i].startswith(("'", '"'))) and where_clause_values[i] != '' and not where_clause_values[i] == 'null':
                                            self.printer.where_incomparable_error()
                                            return
                                        if isinstance(where_clause_values[i], str):
                                            if len(where_clause_values[i]) > int(column_type.split('(')[1][:-1]):
                                                where_clause_values[i] = where_clause_values[i][:int(column_type.split('(')[1][:-1])+1]

                                    if column_type == 'int':
                                        if(not(str(where_clause_values[i]).isdigit()) and where_clause_values[i] != 'null'): 
                                        
                                            self.printer.where_incomparable_error()
                                            return
                                        
                                    if where_clause_values[i] == 'null' and where_clause_operators[i] not in ['is', 'is not']:
                                        self.printer.where_incomparable_error()
                                        return
                                    if where_clause_values[i] != 'null':
                                        if column_type.startswith('char') and where_clause_operators[i] not in ['=', '!=']:
                                            self.printer.where_incomparable_error()
                                            return
                                        elif column_type == 'int' and where_clause_operators[i] not in ['=', '!=', '<', '>', '<=', '>=']:
                                            self.printer.where_incomparable_error()
                                            return
                                        elif column_type == 'date' and where_clause_operators[i] not in ['=', '!=', '<', '>', '<=', '>=']:
                                            self.printer.where_incomparable_error()
                                            return

            # Repeat similar checks for where_clause_attributes2
            for i in range(len(where_clause_attributes2)):
                
                if where_clause_attributes2[i] != '':
                    table_name = None
                    for name, schema in table_schemas.items():
                        if where_clause_attributes2[i] in [column_name for column_name, _ in schema]:
                            table_name = name
                            break
                    if table_name is None:
                        self.printer.where_column_not_exist()
                        return
                    
            
            # Filter the data based on the where clause
            
            # case 1: where clause has only one condition
            
            if boolean_factor_cnt == 1 :
                         
                if const_flags[0] == False:
                    tree_itr = items[2].find_data("boolean_factor")
                    for tree in tree_itr:
                        comp_op_position = str(tree).find("'comp_op'")
                        comparable_value_position = str(tree).find("'comparable_value'")
                    filtered_data = []
                    # Get the column name
                    column_name = where_clause_attributes1[0]
                    # Get the operator
                    operator = where_clause_operators[0]
                    # Get the value
                    value = where_clause_values[0]
                    
                    # Get the table name
                    if where_clause_table_names1[0] != '':
                        table_name = where_clause_table_names1[0]
                    else:
                        table_name = None
                        for name, schema in table_schemas.items():
                            if column_name in [column_name for column_name, _ in schema]:
                                table_name = name
                                
                                break
                    if (table_name+'.'+column_name) in table_column_names[table_names.index(table_name)][1]:
                        column_name = table_name + '.' + column_name
                    
                    if where_clause_attributes2[0] != '':
                        if where_clause_table_names2[0] != '':
                            table_name2 = where_clause_table_names2[0]
                        else:
                            table_name2 = None
                            for name, schema in table_schemas.items():
                                if where_clause_attributes2[0] in [column_name for column_name, _ in schema]:
                                    table_name2 = name
                                    break
                        if (table_name2+'.'+where_clause_attributes2[0]) in table_column_names[table_names.index(table_name2)][1]:
                            where_clause_attributes2[0] = table_name2 + '.' + where_clause_attributes2[0]
                        
                    
                    # Filter the data
                    
                    
                    if isinstance(where_clause_values[0], str) and where_clause_values[0].startswith(("'", '"')):
                        where_clause_values[0] = where_clause_values[0][1:-1]      
                    for row in result_data:
                        
                        # Assume that where_clause_operator is the comparison operator
                        # if the values are string, stripe the quote 
                        
                        if row[column_name] == 'null' and where_clause_operators[0] not in ['is', 'is not']:
                            continue
                        
                        if isinstance(row[column_name], str) and row[column_name].startswith(("'", '"')):
                            row[column_name] = row[column_name][1:-1]
                        if where_clause_attributes2[0] != '':
                            if isinstance(row[where_clause_attributes2[0]], str) and row[where_clause_attributes2[0]].startswith(("'", '"')):
                                row[where_clause_attributes2[0]] = row[where_clause_attributes2[0]][1:-1]
                       
                        if where_clause_attributes2[0] != '':
                            operand2 = where_clause_values[0] if comparable_value_position != -1 else row[where_clause_attributes2[0]]
                        else:
                            operand2 = where_clause_values[0] 
                        
                        if comp_op_position <= comparable_value_position or comparable_value_position == -1: # comp_op comes first       
                            if (comparator(row[column_name],where_clause_operators[0],operand2)):
                                filtered_data.append(row)
                        elif comp_op_position > comparable_value_position: # comparable_value comes first
                        
                            if (comparator(operand2,where_clause_operators[0],row[column_name])):
                                filtered_data.append(row)
                    if filtered_data == []:
                        filtered_data = [{}]
                else:
                    if(where_clause_values[0].startswith(("'", '"'))):
                        where_clause_values[0] = where_clause_values[0][1:-1]
                    if comparator(where_clause_attributes1[0],where_clause_operators[0],where_clause_values[0]):
                        
                        filtered_data = result_data
                    else:
                        filtered_data = [{}]
                
                result_data = filtered_data
                
            
            # case 2: And condition
            elif boolean_term_cnt == 1 and boolean_factor_cnt > 1:
               
                tree_itr = items[2].find_data("boolean_factor")
                comp_op_positions = []
                comparable_value_positions = []
                filtered_data = []
                for tree in tree_itr:
                    comp_op_positions.append(str(tree).find("'comp_op'"))
                    comparable_value_positions.append(str(tree).find("'comparable_value'"))
                values = where_clause_values
                operators = []
                column_names=[]
                table_names = []
              
                for i in range(len(where_clause_attributes1)):
                    
                    operators.append(where_clause_operators[i])
                    table_name = None
                    if where_clause_table_names1[i] != '':
                        table_name = where_clause_table_names1[i]
                        table_names.append(table_name)
                    else:
                        table_name = None
                        for name, schema in table_schemas.items():
                            if where_clause_attributes1[i] in [column_name for column_name, _ in schema]:
                                table_name = name
                                table_names.append(table_name)
                                break
                    
                    # Find the table
                    table = next((table for table in table_column_names if table[0] == table_name), None)

                    if table is not None:
                        # Get the column names of the table
                        table_columns = table[1]            
                        if (table_name+'.'+where_clause_attributes1[i]) in table_columns:
                            where_clause_attributes1[i] = table_name + '.' + where_clause_attributes1[i]
                        column_names.append(where_clause_attributes1[i])
                    else:
                        column_names.append(where_clause_attributes1[i])
                    
                    if where_clause_attributes2[i] != '':
                        if where_clause_table_names2[i] != '':
                            table_name2 = where_clause_table_names2[i]
                        else:
                            table_name2 = None
                            for name, schema in table_schemas.items():
                                if where_clause_attributes2[i] in [column_name for column_name, _ in schema]:
                                    table_name2 = name
                                    break
                        if (table_name2+'.'+where_clause_attributes2[i]) in table_column_names[table_names.index(table_name2)][1]:
                            where_clause_attributes2[i] = table_name2 + '.' + where_clause_attributes2[i]
                # Filter the data
                for i in range(len(values)):
                    
                    if isinstance(values[i], str) and values[i].startswith(("'", '"')):
                        values[i] = values[i][1:-1]
                    
                for row in result_data:
                    
                    conditions_met = []
                    for i in range(len(where_clause_attributes1)):
                        if const_flags[i] == False:# if there is a constant value, compare the constant value with the attribute
                        
                            if row[column_names[i]] == 'null' and operators[i] not in ['is', 'is not']:
                                continue
                            

                            if isinstance(row[column_names[i]], str) and row[column_names[i]].startswith(("'", '"')):
                                row[column_names[i]] = row[column_names[i]][1:-1]
                            if where_clause_attributes2[i] != '':
                                
                                if isinstance(row[where_clause_attributes2[i]], str) and row[where_clause_attributes2[i]].startswith(("'", '"')):
                                    row[where_clause_attributes2[i]] = row[where_clause_attributes2[i]][1:-1]
                            
                            if where_clause_attributes2[i] != '':
                                operand2 = values[i] if comparable_value_positions[i] != -1 else row[where_clause_attributes2[i]]
                            else:
                                operand2 = values[i]
                        

                            if comp_op_positions[i] <= comparable_value_positions[i] or comparable_value_positions[i] == -1:  # comp_op comes first
                                if comparator(row[column_names[i]], operators[i], operand2):
                                    conditions_met.append(True)
                                else:
                                    conditions_met.append(False)
        
                            elif comp_op_positions[i] > comparable_value_positions[i]:  # comparable_value comes first
                                if comparator(operand2, operators[i], row[column_names[i]]):
                                    conditions_met.append(True)
                                else:
                                    conditions_met.append(False)   
                        else: # if there is a constant value, compare the only constant values
                            if where_clause_values[i].startswith(("'", '"')):
                                where_clause_values[i] = where_clause_values[i][1:-1]
                            conditions_met.append(comparator(where_clause_attributes1[i],operators[i],where_clause_values[i]))
                    if all(conditions_met):
                        filtered_data.append(row)
                
                if filtered_data == []:
                    filtered_data = [{}]
                result_data = filtered_data 

            # case 3: Or condition          
            else:
                tree_itr = items[2].find_data("boolean_factor")
                comp_op_positions = []
                comparable_value_positions = []
                filtered_data = []
                for tree in tree_itr:
                    comp_op_positions.append(str(tree).find("'comp_op'"))
                    comparable_value_positions.append(str(tree).find("'comparable_value'"))
                values = where_clause_values
                operators = []
                column_names=[]
                table_names = []
                
                for i in range(len(where_clause_attributes1)):
                    operators.append(where_clause_operators[i])
                    table_name = None
                    if where_clause_table_names1[i] != '':
                        table_name = where_clause_table_names1[i]
                        table_names.append(table_name)
                    else:
                        table_name = None
                        for name, schema in table_schemas.items():
                            if where_clause_attributes1[i] in [column_name for column_name, _ in schema]:
                                table_name = name
                                table_names.append(table_name)
                                break
                    # Find the table
                    table = next((table for table in table_column_names if table[0] == table_name), None)

                    if table is not None:
                        # Get the column names of the table
                        table_columns = table[1]            
                        if (table_name+'.'+where_clause_attributes1[i]) in table_columns:
                            where_clause_attributes1[i] = table_name + '.' + where_clause_attributes1[i]
                        column_names.append(where_clause_attributes1[i])
                    else:
                        column_names.append(where_clause_attributes1[i])
                    
                    if where_clause_attributes2[i] != '':
                        if where_clause_table_names2[i] != '':
                            table_name2 = where_clause_table_names2[i]
                        else:
                            table_name2 = None
                            for name, schema in table_schemas.items():
                                if where_clause_attributes2[i] in [column_name for column_name, _ in schema]:
                                    table_name2 = name
                                    break
                        if (table_name2+'.'+where_clause_attributes2[i]) in table_column_names[table_names.index(table_name2)][1]:
                            where_clause_attributes2[i] = table_name2 + '.' + where_clause_attributes2[i]
                # Filter the data
                for i in range(len(values)):
                    if isinstance(values[i], str) and values[i].startswith(("'", '"')):
                        values[i] = values[i][1:-1]
                    
                    
                
                for row in result_data:
                    conditions_met = []
                    
                    for i in range(len(where_clause_attributes1)):
                        # if there is a constant value, compare the constant value with the attribute
                        if const_flags[i] == False:         
                            if row[column_names[i]] == 'null' and operators[i] not in ['is', 'is not']:
                                continue
                           
                            if isinstance(row[column_names[i]], str) and row[column_names[i]].startswith(("'", '"')):
                                row[column_names[i]] = row[column_names[i]][1:-1]
                            if where_clause_attributes2[i] != '':
                                
                                if isinstance(row[where_clause_attributes2[i]], str) and row[where_clause_attributes2[i]].startswith(("'", '"')):
                                    row[where_clause_attributes2[i]] = row[where_clause_attributes2[i]][1:-1]
                           
                            if where_clause_attributes2[i] != '':
                                operand2 = values[i] if comparable_value_positions[i] != -1 else row[where_clause_attributes2[i]]
                            else:
                                operand2 = values[i]
                           

                            if comp_op_positions[i] <= comparable_value_positions[i] or comparable_value_positions[i] == -1:  # comp_op comes first
                                if comparator(row[column_names[i]], operators[i], operand2):
                                    conditions_met.append(True)
                                else:
                                    conditions_met.append(False)
        
                            elif comp_op_positions[i] > comparable_value_positions[i]:  # comparable_value comes first
                                if comparator(operand2, operators[i], row[column_names[i]]):
                                    conditions_met.append(True)
                                else:
                                    conditions_met.append(False)
                        else: # if there is a constant value, compare the only constant values
                            if where_clause_values[i].startswith(("'", '"')):
                                where_clause_values[i] = where_clause_values[i][1:-1]
                            conditions_met.append(comparator(where_clause_attributes1[i],operators[i],where_clause_values[i]))
                    if any(conditions_met):
                        filtered_data.append(row)
                # If there are no rows that satisfy the conditions, return an empty dictionary
                if filtered_data == []:
                    filtered_data = [{}]
                result_data = filtered_data


            
        
        # SELECT *
        if isinstance(items[1], Tree) and items[1].data == "select_list" and len(items[1].children) == 0:
            try:
                
                # if there are same name of columns in different tables, add the prefix of table name
                # to the column name
                # Flatten the list of column names
                all_column_names = [col_name for table in table_column_names for col_name in table[1]]

                # Check for duplicates
                duplicates = set([name for name in all_column_names if all_column_names.count(name) > 1])
                
                # Add table name prefix to duplicate column names
                for table in table_column_names:
                    table_name, column_names = table
                    for i, col_name in enumerate(column_names):
                        if col_name in duplicates:
                            # Add table name as prefix
                            column_names[i] = f"{table_name}.{col_name}"


                column_names = [column for table, columns in table_column_names for column in columns]
                
                # Get schema
                schema_data = self.my_schema_db.get(table_name.encode())
                
                schema = pickle.loads(schema_data)
                
            except pickle.UnpicklingError:
                print("Data is not pickle data")

            
            # Extract column names from table_column_names
            
            
            

        # SELECT column_name for project 1-3
        elif isinstance(items[1], Tree) and items[1].data == "select_list":      
            #Get the schemas of the tables
            schemas = [pickle.loads(self.my_schema_db.get(table_name.encode())) for table_name in table_names]
            selected_column_iter = items[1].find_data("selected_column")
            query_column_names = []
            query_table_names = []

            for selected_column in selected_column_iter:
                query_column_name_iter = selected_column.find_data("column_name")
                query_column_names += [child.children[0].value.lower() for child in query_column_name_iter]
                query_table_name_iter = selected_column.find_data("table_name")
                query_table_name = ''
                for child in query_table_name_iter:
                    query_table_name = child.children[0].value.lower()
                query_table_names += [query_table_name]
            
            
            # Check if the column names exist in the schema
            column_names_dict = {table_name: [column_name for column_name, _ in schema] for table_name, schema in zip(table_names, schemas)}
            for column_name in query_column_names:
                if not any(column_name in column_names for column_names in column_names_dict.values()):
                    self.printer.select_column_resolve_error(column_name)
                    return

            # Check if the column names are ambiguous
            for index in range(len(query_column_names)):
                
                #table_name is explicitly specified
                if query_table_names[index] != '':
                    if query_table_names[index] not in table_names:
                        self.printer.select_column_resolve_error(query_table_names[index]+'.'+query_column_names[index])
                        return
                    temp_table_name = query_table_names[index]
                    #extract column names from schema which corresponds to the table_name
                    schema = pickle.loads(self.my_schema_db.get(temp_table_name.encode()))
                    selected_column_names = [column_name for column_name, _ in schema]
                    
                    
                    if query_column_names[index] not in selected_column_names:
                        self.printer.select_column_resolve_error(query_column_names[index])
                        return
                    
                else: #table_name is not specified
                    count = 0
                      
                    for selected_column_names in column_names_dict.values():
                        if query_column_names[index] in selected_column_names:
                            count += 1
                    if count > 1:    
                        self.printer.select_column_resolve_error(query_column_names[index])
                        return
                
            all_column_names = [col_name for table in table_column_names for col_name in table[1]]
            for i in range(len(query_column_names)):
                if query_column_names[i] not in all_column_names: #if ambiguous
                    updated_column_name = query_table_names[i] + '.' + query_column_names[i]
                    query_column_names[query_column_names.index(query_column_names[i])] = updated_column_name
    
            column_names = query_column_names
            
            if result_data != [{}]:
                result_data = [{column_name: row[column_name] for column_name in column_names} for row in result_data]

            
            
        # Calculate max length for each column
        
        try:
            if result_data[0]:
                max_lengths = []
                for column in column_names:
                    if column in result_data[0]:  # Check if the column exists
                        max_length = len(column)  # Initialize with the length of the column name
                        for row in result_data:
                            value = row[column]
                            if isinstance(value, Token):
                                length = len(str(value.value if value is not None else ''))
                            else:
                                length = len(str(value if value is not None else ''))
                            max_length = max(max_length, length)
                        max_lengths.append(max_length)
                    else:
                        max_lengths.append(len(column))
                        
            else:
                max_lengths = [len(column) for column in column_names]
        except KeyError as e:
            print(f"KeyError: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        
        for column, max_length in zip(column_names, max_lengths):
            print('+' + '-' * (max_length + 2), end='')
        print('+')
        for column, max_length in zip(column_names, max_lengths):
            print('| ' + column.upper().ljust(max_length) + ' ', end='')
        print('|')
        for column, max_length in zip(column_names, max_lengths):
            print('+' + '-' * (max_length + 2), end='')
        print('+')
        if result_data[0]:
            for row in result_data:
                for column, max_length in zip(column_names, max_lengths):
                    if column in row:  # Check if the column exists
                        value = str(row[column]) if row[column] is not None else 'NULL'  # Get the value directly from row[column]
                        if isinstance(row[column], str):  # if the value is a string, remove the quotes
                            value = value.strip('\'"')
                    else:
                        value = ''
                    print('| ' + value.ljust(max_length) + ' ', end='')
                print('|')
        for column, max_length in zip(column_names, max_lengths):
            print('+' + '-' * (max_length + 2), end='')
        print('+')

            
            
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
            elif isinstance(item, Token) and item.type in ['INT', 'STR', 'DATE.9']:
                values.append(str(item))
        

        

        # Check if the table exists
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.no_such_table()
            return

        # Get the schema of the table
        table_schema = pickle.loads(self.my_schema_db.get(table_name.encode()))
        schema_column_names = [column_name for column_name, _ in table_schema]

        
        # Get column names and values from query
        # when Explicit column names are provided
        if items[3] != None:
            query_column_names = [child.children[0].value.lower() for child in items[3].children if isinstance(child, Tree)]
            
            # Check if all column names exist in the schema
            for column_name in query_column_names:
                if column_name not in schema_column_names:
                    self.printer.insert_column_existence_error(column_name)
                    return
            # Check if the number of values matches the number of columns
            if len(values) != len(query_column_names):
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
                    self.printer.insert_column_non_nullable_error(column_name)
                    return
                
                if column_type.startswith('char'):
                    if value is None:
                        values[i] = "null"
                    # Check if the value is a string
                    # if value does not startswith a quote, it is not a string
                    
                    elif value.startswith(("'", '"')) == False:
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
                    value = values[index]
                    row_data[column_name] = value
                # If the column is not in the query, set it to "null"   
                else:
                    row_data[column_name] = "null"
            # Add row_data to table_datainsert into students (id) values ('20141234');
            
            table_data = pickle.loads(self.my_data_db.get(table_name.encode(), pickle.dumps([])))
            table_data.append(row_data)

            # Update the table in the database
            self.my_data_db.put(table_name.encode(), pickle.dumps(table_data))

            # Print success message
            self.printer.insert_result(1)
            return

        # Check if the values are valid
        # Check if the number of values matches the number of columns
        if len(values) != len(table_schema):
            self.printer.insert_value_error()
            return
        for i, (column_name, (column_type, column_null, column_key, column_reference)) in enumerate(table_schema):
            value = values[i]
            if column_null == 'N' and value is None:
                self.printer.insert_column_non_nullable_error(column_name)
                return
            if column_type.startswith('char'):
                if value is None:
                    values[i] = "null"
                elif value.startswith(("'", '"')) == False:
                    self.printer.insert_value_error()
                    return
                if isinstance(value, str):
                    if len(value) > int(column_type.split('(')[1][:-1]):
                        # Update the value to its truncated version
                        value = value[:int(column_type.split('(')[1][:-1])+1]
                        # Update the value in the values list
                        values[i] = value
            if column_type == 'int' and value is None:
                values[i] = "null"

            elif column_type == 'int' and not str(value).isdigit():
                self.printer.insert_value_error()
                return
            

            # date type should be in this format"YYYY-MM-DD"
            if column_type == 'date' and value is not None:
                if is_valid_date_format(value, date_format) == False:
                    self.printer.insert_value_error()
                    return
            if column_type == 'date' and value is None:
                values[i] = "null"
            
            
        # Prepare the row data as a dictionary
        row_data = {column_name: value for (column_name, _), value in zip(table_schema, values)}

        # Insert the values into the table
        table_data = pickle.loads(self.my_data_db.get(table_name.encode(), pickle.dumps([])))
        table_data.append(row_data)
        self.my_data_db.put(table_name.encode(), pickle.dumps(table_data))

        # Print success message
        self.printer.insert_result(1)


    def delete_query(self, items):  
        table_name = items[2].children[0].lower()        
        # Check if the table existsS
        if not self.my_schema_db.exists(table_name.encode()):
            self.printer.no_such_table()
            return
        # if there is no where clause, delte all rows
        if items[3] == None:
            try:
                count = 0
                table_data = pickle.loads(self.my_data_db.get(table_name.encode()))
                for row in table_data:
                    count += 1
                self.my_data_db.delete(table_name.encode())
                self.printer.delete_result(count)
                return
            except:
                self.printer.delete_result(0)
                return

        # else, delete rows that satisfies the condition
        # char type attribute can be compared with =, != only
        # int type attribute can be compared with =, !=, <, >, <=, >=
        # date type attribute can be compared with =, !=, <, >, <=, >=
        # null type can be compared with all other types(is, is not)
        else:
            if items[3].children[0].lower():
                #boolean term = 1 && boolean factor = 2 -> AND
                #boolean factor = 2 -> OR
                #boolean term = 1 && boolean factor = 1 -> just 1 where clause   

                boolean_factor_iter = items[3].children[1].find_data("boolean_factor")
                boolean_term_iter = items[3].children[1].find_data("boolean_term")
                boolean_factor_cnt = 0
                boolean_term_cnt = 0
                for _ in boolean_term_iter:
                    boolean_term_cnt += 1
                for _ in boolean_factor_iter:
                    boolean_factor_cnt += 1

                where_clause_table_name_iter = items[3].children[1].find_data("table_name")
                where_clause_attribute_iter = items[3].children[1].find_data("column_name")
                where_clause_operator_iter = items[3].children[1].find_data("comp_op")
                where_clause_value_iter = items[3].children[1].find_data("comparable_value")
                where_clause_null_iter = items[3].children[1].find_data("null_operation")
                
                
                # Extract the attribute, operator, and value from the where clause
                where_clause_table_names = []
                where_clause_attributes = []
                where_clause_operators = []
                where_clause_values = []

                where_clause_table_names = where_clause_table_names + [child.children[0].value.lower() for child in where_clause_table_name_iter]
                where_clause_attributes = where_clause_attributes + [child.children[0].value.lower() for child in where_clause_attribute_iter]             
                where_clause_values = where_clause_values + [child.children[0].value for child in where_clause_value_iter]
                where_clause_operators = where_clause_operators + [child.children[0].value.lower() for child in where_clause_operator_iter]
                
                for child in where_clause_null_iter:
                    where_clause_values =where_clause_values + ['null']
                    if child.children[1] is None:
                        where_clause_operators = where_clause_operators + [child.children[0].value]
                    else:
                        where_clause_operators = where_clause_operators + [child.children[0].value+' '+child.children[1].value] 
        

                # Get the schema of the table
                table_schema = pickle.loads(self.my_schema_db.get(table_name.encode()))
                schema_column_names = [column_name for column_name, _ in table_schema]
                # Get the table data
                table_data = pickle.loads(self.my_data_db.get(table_name.encode()))
                # Delete the rows that satisfy the condition
                count = 0
                new_table_data = []

                for specified_table_name in where_clause_table_names:
                    if specified_table_name != table_name:
                        self.printer.where_table_not_specified()
                        return
                
                for i in range(len(where_clause_attributes)):
                    # Check if the column exists in the schema
                    
                    if where_clause_attributes[i] not in schema_column_names:
                        self.printer.where_column_not_exist()
                        return
                    # Check if the value is valid
                    for column_name, (column_type, column_null, column_key, column_reference) in table_schema:
                        if column_name == where_clause_attributes[i]:
                            if column_type.startswith('char'):
                                if where_clause_values[i] is None:
                                    where_clause_values[i] = "null"
                                elif not isinstance(where_clause_values[i], str):
                                    self.printer.where_incomparable_error()
                                    return
                                if isinstance(where_clause_values[i], str):
                                    if len(where_clause_values[i]) > int(column_type.split('(')[1][:-1]):
                                        # Update the value to its truncated version
                                        where_clause_values[i] = where_clause_values[i][:int(column_type.split('(')[1][:-1])+1]
                                        # Update the value in the values list
                                        where_clause_values[i] = where_clause_values[i]
                            if column_type == 'int' and (not(str(where_clause_values[i]).isdigit()) and where_clause_values[i] != 'null'):                
                                self.printer.where_incomparable_error()
                                return
                            
                            if(where_clause_values[i] == 'null' and where_clause_operators[i] not in ['is', 'is not']):
                                
                                self.printer.where_incomparable_error()
                                return
                            if(where_clause_values[i] != 'null'):
                                if column_type.startswith('char') and where_clause_operators[i] not in ['=', '!=']:
                                
                                    self.printer.where_incomparable_error()
                                    return
                                
                                elif column_type == 'int' and where_clause_operators[i] not in ['=', '!=', '<', '>', '<=', '>=']:
                                    
                                    self.printer.where_incomparable_error()
                                    return
                                elif column_type == 'date' and where_clause_operators[i] not in ['=', '!=', '<', '>', '<=', '>=']:
                                   
                                    self.printer.where_incomparable_error()
                                    return
                            
        
               
                if(boolean_factor_cnt == 1 & boolean_term_cnt == 1): # just one where clause
                   
                    for row in table_data:
                        # Assume that where_clause_operator is the comparison operator
                        # if the values are string, stripe the quote 
                        if where_clause_values[0].startswith(("'", '"')) :
                            where_clause_values[0] = where_clause_values[0][1:-1]
                        if row[where_clause_attributes[0]].startswith(("'", '"')) :
                            row[where_clause_attributes[0]] = row[where_clause_attributes[0]][1:-1]

                        tree_str = str(items[3].children[1].children[0].children[0])
                        comp_op_position = tree_str.find("'comp_op'")
                        comparable_value_position = tree_str.find("'comparable_value'")
                        
                        if comp_op_position <= comparable_value_position: # comp_op comes first
                            if (not(comparator(row[where_clause_attributes[0]],where_clause_operators[0],where_clause_values[0]))):
                                new_table_data.append(row)
                            else:
                                count += 1
                        elif comp_op_position > comparable_value_position: # comparable_value comes first
                            if (not(comparator(where_clause_values[0],where_clause_operators[0],row[where_clause_attributes[0]]))):
                                new_table_data.append(row)
                            else:
                                count += 1
    
                    # Update the table in the database
                    self.my_data_db.put(table_name.encode(), pickle.dumps(new_table_data))
                    # Print success message
                    self.printer.delete_result(count)
                    return
                # AND
                elif(boolean_factor_cnt == 2 and boolean_term_cnt == 1):
                    tree_str0 = str(items[3].children[1].children[0].children[0])
                    tree_str1 = str(items[3].children[1].children[0].children[2])
                    comp_op_position0 = tree_str0.find("'comp_op'")
                    comparable_value_position0 = tree_str0.find("'comparable_value'")
                    comp_op_position1 = tree_str1.find("'comp_op'")
                    comparable_value_position1 = tree_str1.find("'comparable_value'")
                    
                    for row in table_data:
                        # Assume that where_clause_operator is the comparison operator
                        # if the values are string, stripe the quote 
                        if where_clause_values[0].startswith(("'", '"')) :
                            where_clause_values[0] = where_clause_values[0][1:-1]
                        if row[where_clause_attributes[0]].startswith(("'", '"')) :
                            row[where_clause_attributes[0]] = row[where_clause_attributes[0]][1:-1]

                        if where_clause_values[1].startswith(("'", '"')) :
                            where_clause_values[1] = where_clause_values[1][1:-1]
                        if row[where_clause_attributes[1]].startswith(("'", '"')) :
                            row[where_clause_attributes[1]] = row[where_clause_attributes[1]][1:-1]

                        if comp_op_position0 <= comparable_value_position0: # comp_op comes first
                            if comp_op_position1 <= comparable_value_position1: # comp_op comes first
                                if(not(comparator(row[where_clause_attributes[0]], where_clause_operators[0], where_clause_values[0])&
                                 comparator(row[where_clause_attributes[1]], where_clause_operators[1], where_clause_values[1]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1
                            elif comp_op_position1 > comparable_value_position1: # comparable_value comes first
                                if(not(comparator(row[where_clause_attributes[0]], where_clause_operators[0], where_clause_values[0])&
                                 comparator(where_clause_values[1], where_clause_operators[1], row[where_clause_attributes[1]]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1
                        elif comp_op_position0 > comparable_value_position0: # comparable_value comes first
                            if comp_op_position1 <= comparable_value_position1:
                                if(not(comparator(where_clause_values[0], where_clause_operators[0], row[where_clause_attributes[0]])&
                                 comparator(row[where_clause_attributes[1]], where_clause_operators[1], where_clause_values[1]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1
                            elif comp_op_position1 > comparable_value_position1:
                                if(not(comparator(where_clause_values[0], where_clause_operators[0], row[where_clause_attributes[0]])&
                                 comparator(where_clause_values[1], where_clause_operators[1], row[where_clause_attributes[1]]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1

                        
                    # Update the table in the database
                    self.my_data_db.put(table_name.encode(), pickle.dumps(new_table_data))
                    # Print success message
                    self.printer.delete_result(count)
                    return
                # OR
                else:
                    tree_str0 = str(items[3].children[1].children[0])
                    
                    tree_str1 = str(items[3].children[1].children[2])
               
                    comp_op_position0 = tree_str0.find("'comp_op'")
                    comparable_value_position0 = tree_str0.find("'comparable_value'")
                    comp_op_position1 = tree_str1.find("'comp_op'")
                    comparable_value_position1 = tree_str1.find("'comparable_value'")
                   
                    for row in table_data:
                        # Assume that where_clause_operator is the comparison operator
                        # if the values are string, stripe the quote 
                        if where_clause_values[0].startswith(("'", '"')) :
                            where_clause_values[0] = where_clause_values[0][1:-1]
                        if row[where_clause_attributes[0]].startswith(("'", '"')) :
                            row[where_clause_attributes[0]] = row[where_clause_attributes[0]][1:-1]

                        if where_clause_values[1].startswith(("'", '"')) :
                            where_clause_values[1] = where_clause_values[1][1:-1]
                        if row[where_clause_attributes[1]].startswith(("'", '"')) :
                            row[where_clause_attributes[1]] = row[where_clause_attributes[1]][1:-1]

                        if comp_op_position0 <= comparable_value_position0: # comp_op comes first
                            if comp_op_position1 <= comparable_value_position1: # comp_op comes first
                                if(not(comparator(row[where_clause_attributes[0]], where_clause_operators[0], where_clause_values[0])|
                                 comparator(row[where_clause_attributes[1]], where_clause_operators[1], where_clause_values[1]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1
                            elif comp_op_position1 > comparable_value_position1: # comparable_value comes first
                                if(not(comparator(row[where_clause_attributes[0]], where_clause_operators[0], where_clause_values[0])|
                                 comparator(where_clause_values[1], where_clause_operators[1], row[where_clause_attributes[1]]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1
                        elif comp_op_position0 > comparable_value_position0: # comparable_value comes first
                            if comp_op_position1 <= comparable_value_position1:
                                if(not(comparator(where_clause_values[0], where_clause_operators[0], row[where_clause_attributes[0]])|
                                 comparator(row[where_clause_attributes[1]], where_clause_operators[1], where_clause_values[1]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1
                            elif comp_op_position1 > comparable_value_position1:
                                if(not(comparator(where_clause_values[0], where_clause_operators[0], row[where_clause_attributes[0]])|
                                 comparator(where_clause_values[1], where_clause_operators[1], row[where_clause_attributes[1]]))):
                                    new_table_data.append(row)
                                else:
                                    count += 1
                    # Update the table in the database
                    self.my_data_db.put(table_name.encode(), pickle.dumps(new_table_data))
                    # Print success message
                    self.printer.delete_result(count)
                    return

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
    if(flag): 
        tmp = input()
    else:
        try:
            tmp = input(prompt)
        except EOFError:
            print("EOF detected. Exiting the program.")
            break  # or sys.exit(), depending on your needs
    input_str += tmp
    if(not input_str.endswith(';') or input_str == ''):
        flag=True
        continue
    if input_str.endswith(';'):
        query_list = [s.strip() for s in input_str.split(';') if s.strip()]
        if all(q == '' for q in query_list):
            print(prompt+"Syntax error")
            input_str = ""
            flag = False
            continue
        for q in query_list:
            try:
                output = sql_parser.parse(q+';')
                MyTransformer(prompt).transform(output)
            except Exception as e:
                print(prompt+"Syntax error")
                input_str = ""
                flag=False
                break
        input_str=''
        flag=False
                     

                 