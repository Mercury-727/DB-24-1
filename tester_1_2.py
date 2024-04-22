test_cases = [
    {
        "query": """create table dummy (
            id int    
        );""",
        "expected": "'dummy' table is created",
        "test_name": "create table without constraints",
    },
    {
        "query": """create table user (
            id int, 
            name char(15), 
            gender char(1),
            primary key (id)
        );""",
        "expected": "'user' table is created",
        "test_name": "create table user"
    },
    {
        "query": """create table role (
            id int, 
            name char(15), 
            description char(100),
            primary key (id)
        );""",
        "expected": "'role' table is created",
        "test_name": "create table role"
    },
    {
        "query": """create table tuple_pk_table (
            id int, 
            name char(15), 
            description char(100) not null,
            primary key (id, name)
        );""",    
        "expected": "'tuple_pk_table' table is created",
        "test_name": "create table tuple_pk_table"
    },
    {
        "query": """create table referencing_table (
            id char(10),
            role_id int,
            primary key (id),
            foreign key (role_id) references role (id)    
        );""",
        "expected": "'referencing_table' table is created",
        "test_name": "create table referencing_table"
    },
    {
        "query": """create table referencing_table_tuple_fk (
            id int,
            name char(15) not null,
            tuple_fk_id int,
            tuple_fk_name char(15),
            primary key (id, tuple_fk_id),
            foreign key (tuple_fk_id, tuple_fk_name) references tuple_pk_table (id, name)    
        );""",
        "expected": "'referencing_table_tuple_fk' table is created",
        "test_name": "create table referencing_table_tuple_fk",
    },
    {
        "query": """create table table_to_drop(
            id int,
            name char(15),
            primary key (id)
        );""",
        "expected": "'table_to_drop' table is created",
        "test_name": "create table table_to_drop"
    },
    { 
        "query": "create table test (id int, id int);",
        "expected": "Create table has failed: column definition is duplicated",
        "test_name": "duplicate column"
    },
    { 
        "query": "create table test (id int, name char(15), primary key (id), primary key (name));",
        "expected": "Create table has failed: primary key definition is duplicated",
        "test_name": "duplicate primary key"
    },
    { 
        "query": """create table test (
            id int, 
            name char(15), 
            char_id char(15),
            primary key (id),
            foreign key (char_id) references role (id)
        );""",
        "expected": "Create table has failed: foreign key references wrong type",
        "test_name": "foreign key references wrong type"
    },
    { 
        "query": """create table test (
            id int, 
            name char(15), 
            role_id int,
            primary key (id),
            foreign key (name) references role (name)
        );""",
        "expected": "Create table has failed: foreign key references non primary key column",
        "test_name": "foreign key references non primary key column"
    },
    { 
        "query": """create table test (
            id int,
            name char(15),
            foreign key (id) references tuple_pk_table (id)    
        );""",
        "expected": "Create table has failed: foreign key references non primary key column",
        "test_name": "foreign key references primary key, but not all columns"
    },
    { 
        "query": """create table test (
            id int,
            name char(15),
            foreign key (id) references role (non_existing_column)    
        );""",
        "expected": "Create table has failed: foreign key references non existing column",
        "test_name": "foreign key references non existing column"
    },
    { 
        "query": """create table test (
            id int,
            name char(15),
            foreign key (id) references non_existing_table (id)    
        );""",
        "expected": "Create table has failed: foreign key references non existing table",
        "test_name": "foreign key references non existing table"
    },
    { 
        "query": """create table test (
            id int,
            name char(15),
            primary key (non_existing_column)
        );""",
        "expected": "Create table has failed: 'non_existing_column' does not exist in column definition",
        "test_name": "primary key column does not exist in column definition"
    },
    { 
        "query": """create table test (
            id int,
            name char(15),
            foreign key (non_existing_column) references role (id)    
        );""",
        "expected": "Create table has failed: 'non_existing_column' does not exist in column definition",
        "test_name": "foreign key column does not exist in column definition"
    },
    { 
        "query": """create table user (
            id int, 
            name char(15),
            primary key (id)
        );""",
        "expected": "Create table has failed: table with the same name already exists",
        "test_name": "duplicate table name"
    },
    { 
        "query": """create table test (
            id int,
            name char(0),
            primary key (id)
        );""",
        "expected": "Char length should be over 0",
        "test_name": "char length should be over 0"
    },
    { 
        "query": """create table test (
            id int,
            name char(-1),
            primary key (id)
        );""",
        "expected": "Char length should be over 0",
        "test_name": "char length should be over 0"
    },
    {
        "query": "select * from user;",
        "expected": """
        ID NAME GENDER
        """,
        "test_name": "select empty table",  
    },
    
    {
        "query": """explain user;""",
        "expected": """
        table_name [user]
        column_name type null key
        id int N PRI
        name char(15) Y
        gender char(1) Y
        """,
        "test_name": "explain user"
    },
    {
        "query": """explain role;""",
        "expected": """
        table_name [role]
        column_name type null key
        id int N PRI
        name char(15) Y
        description char(100) Y
        """,
        "test_name": "explain role"
    },
    {
        "query": """explain tuple_pk_table;""",
        "expected": """
        table_name [tuple_pk_table]
        column_name type null key
        id int N PRI
        name char(15) N PRI
        description char(100) N
        """,
        "test_name": "explain tuple_pk_table"
    },
    {
        "query": """explain referencing_table;""",
        "expected": """
        table_name [referencing_table]
        column_name type null key
        id char(10) N PRI
        role_id int Y FOR
        """,
        "test_name": "explain referencing_table"
    },
    {
        "query": """explain referencing_table_tuple_fk;""",
        "expected": """
        table_name [referencing_table_tuple_fk]
        column_name type null key
        id int N PRI
        name char(15) N
        tuple_fk_id int N PRI/FOR
        tuple_fk_name char(15) Y FOR
        """,
        "test_name": "explain referencing_table_tuple_fk"
    },
    {
        "query": """desc referencing_table_tuple_fk;""",
        "expected": """
        table_name [referencing_table_tuple_fk]
        column_name type null key
        id int N PRI
        name char(15) N
        tuple_fk_id int N PRI/FOR
        tuple_fk_name char(15) Y FOR
        """,
        "test_name": "desc referencing_table_tuple_fk"
    },
    {
        "query": """describe referencing_table_tuple_fk;""",
        "expected": """
        table_name [referencing_table_tuple_fk]
        column_name type null key
        id int N PRI
        name char(15) N
        tuple_fk_id int N PRI/FOR
        tuple_fk_name char(15) Y FOR
        """,
        "test_name": "describe referencing_table_tuple_fk"
    },
    {
        "query": """describe non_existing_table;""",
        "expected": "No such table",
        "test_name": "describe non-existing table"
    },
    {
        "query": """desc non_existing_table;""",
        "expected": "No such table",
        "test_name": "desc non-existing table"
    },
    {
        "query": """explain non_existing_table;""",
        "expected": "No such table",
        "test_name": "explain non-existing table"
    },
    {
        "query": """show tables;""",
        "expected": """
        user
        role
        referencing_table_tuple_fk
        referencing_table
        tuple_pk_table
        table_to_drop
        dummy
        """,
        "test_name": "show tables"
    },
    {
        "query": "drop table table_to_drop;",
        "expected": "'table_to_drop' table is dropped",
        "test_name": "drop table table_to_drop"
    },
    {
        "query": "drop table table_to_drop;",
        "expected": "No such table",
        "test_name": "drop non-existing table"
    },
    {
        "query": "drop table role;",
        "expected": "Drop table has failed: 'role' is referenced by other table",
        "test_name": "drop table role referenced by other table"
    },
    {
        "query": """create table table_to_drop (
            id int,
            name char(15),
            primary key (id)
        );""",
        "expected": "'table_to_drop' table is created",
        "test_name": "recreate table that was dropped"
    },
    {
        "query": """insert into user values (1, 'Alice', 'F');""",
        "expected": "The row is inserted",
        "test_name": "insert into user",
    },
    {
        "query": """insert into user (id, gender, name) values (2, 'M', 'Bob');""",
        "expected": "The row is inserted",
        "test_name": "insert into user with column order changed",
    },
    {
        "query": """insert into user values (3, null, null);""",
        "expected": "The row is inserted",
        "test_name": "insert into user with null values",
    },
    {
        "query": """insert into user values (4, "extremelylonglonglonglonglonglonglonglongname", 'FF');""",
        "expected": "The row is inserted",
        "test_name": "insert into user with long char value",
    },
    {
        "query": """insert into user values (5, 'null', 'F');""",
        "expected": "The row is inserted",
        "test_name": "insert into user with string 'null'",
    },
    {
        "query": """insert into nonexist_table values (1, 'Alice', 'F');""",
        "expected": "No such table",
        "test_name": "insert into nonexist_table",
    },
    {
        "query": "select * from user;",
        "expected": """
        ID NAME GENDER
        1 Alice F
        2 Bob M
        3 null null
        4 extremelylonglo F
        5 null F
        """,
        "test_name": "select * from user checking (null, long char, truncated null)"
    },
    {
        "query": "select * from nonexist_table;",
        "expected": "Selection has failed: 'nonexist_table' does not exist",
        "test_name": "select * from non-existing table"  
    },
    {
        "query": "exit;",
        "expected": "",
        "test_name": "exit"
    }
]

'''
test cases to be add in project 1-3
- insert null value to primary key column
- insert unmatched type value to column (including different char length)
- insert null to nullable fk column
- self referencing fk
- case senisitity
- duplicate column in insert query
- unmatched column count in insert query
- unmatched column type in insert query
- insert null value to not null column
'''

import subprocess, sys, argparse, os, re
PREFIX = ""
DEFAULT_ID = "1234-56789"

parser = argparse.ArgumentParser()
parser.add_argument("--id", type=str, help="Please set your student id. e.g. --id 1234-56789")


def setup():
    # if user replies 'y', remove all db files in this directory
    print("Note: This test can run only in linux environment. (line separator is \\n)")
    print("Note: This test assume that no db files are required before running run.py.")
    print("      If you have some db files that should be created before running,")
    print("      you should **EDIT THIS tester.py FILE** to add some setup code.")
    print("      I commented where you should edit with FIXME.")
    print("Note: Ignore dashed lines, white spaces and the order when evaluating desc/explain/describe/select/show query.")
    required_db_files = ["example.db", "example2.db"] # FIXME: add your required db files here
    
    user_input = input("Do you want to remove all db files in this directory before starting test? (y/n): ").strip()
    
    # remove all db files in this directory and subdirectories
    if user_input.lower() == "y":
        for root, dirs, files in os.walk("."):
            for file in files:
                if file.endswith(".db"):
                    # FIXME comment out this line if you have some db files that should be created before running
                    # if file in required_db_files:
                        # continue
                    
                    os.remove(os.path.join(root, file))
        print("All db files are removed.")
        return True
    else:
        print("Cannot start test without removing db files. Backup your db files and try again.")
        return False

def test(test_cases):
    for i, test_case in enumerate(test_cases):
        popen = subprocess.Popen(["python3", "run.py"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        query = re.sub(r"\s+", " ", " ".join(test_case["query"].split("\n")))
        if i != len(test_cases) - 1:
            query += " exit;"
        expected = test_case["expected"]
        test_name = test_case.get("test_name", None)
        
        std_out, std_err = popen.communicate(input=(query + "\n").encode("utf-8"))
        std_out, std_err = (std_out).decode("utf-8"), (std_err).decode("utf-8")

        if std_err:
            print("Error occurred while running run.py")
            print(std_err)
            return False

        # remove prefix
        # print("tst qury", query)
        # print("test stdout: ", std_out)
        result = std_out[len(PREFIX):].strip()  
        # print("test result:", result)  
        
        print(f"--------Test Case {i + 1} Start--------")
        describe_quries = ["explain", "desc", "describe"]
        show_query = ["show"]
        select_query = ["select"]
        
        if query != "exit;":
            query = query[:-len(" exit;")]
        

        print("     Note: Ignore dashed lines, white spaces and the order when evaluating desc/explain/describe/select/show query.")
        if any([query.lower().startswith(q) for q in describe_quries]) and expected != "No such table":
            expected = expected.strip().split("\n")
            expected = [line.strip() for line in expected]
            # substitute multiple spaces with single space
            expected = [re.sub(r"\s+", " ", line) for line in expected]
            expected[1:].sort()
            
            result = result.strip().split("\n")
            result = [line.strip() for line in result]
            start_line, end_line = result[0], result[-1]
            result = result[1:-1]
            # substitute multiple spaces with single space
            result = [re.sub(r"\s+", " ", line) for line in result]
            result[1:].sort()
            
            print(f"Test Name: {test_name}")
            print(f"    Query: {query}")


            print(f"   Output:")
            for r in result:
                print(f"    {r}")
            print(f" Expected:")
            for e in expected:
                print(f"    {e}")
            assert result == expected
            
            print("Dashed Line Check:")
            is_dashed_line = lambda line: re.match(r"^-+$", line)
            print(f"      start_line: {bool(is_dashed_line(start_line))}, end_line: {bool(is_dashed_line(end_line))}")
            assert is_dashed_line(start_line) and is_dashed_line(end_line)
        elif any([query.lower().startswith(q) for q in show_query]):
            expected = expected.strip().split("\n")
            expected = [line.strip() for line in expected]
            expected.sort()
            
            result = result.strip().split("\n")
            result = [line.strip() for line in result]
            start_line, end_line = result[0], result[-1]
            result = result[1:-1]
            result.sort()
            
            print(f"Test Name: {test_name}")
            print(f"    Query: {query}")
            print(f"   Output: {result}")
            print(f" Expected: {expected}")
            assert result == expected
            
            print("Dashed Line Check:")
            is_dashed_line = lambda line: re.match(r"^-+$", line)
            print(f"      start_line: {bool(is_dashed_line(start_line))}, end_line: {bool(is_dashed_line(end_line))}")
            assert is_dashed_line(start_line) and is_dashed_line(end_line)
            
        elif any([query.lower().startswith(q) for q in select_query]) and not expected.startswith("Selection has failed"):
            # select
            expected = expected.strip().split("\n")
            expected = [line.strip() for line in expected]
            expected = [re.sub(r"\s+", " ", line) for line in expected]
            expected[1:].sort()
            
            result = result.strip().split("\n")
            # remove | character
            result = [re.sub(r"\|", " ", line) for line in result]
            result = [line.strip() for line in result]
            start_header_line, end_header_line, end_line = result[0], result[2], result[-1]
            result = [result[1]] + result[3:-1]
            # substitute multiple spaces with single space
            result = [re.sub(r"\s+", " ", line) for line in result]
            result[1:].sort()
            
            print(f"Test Name: {test_name}")
            print(f"    Query: {query}")
            print(f"   Output:")
            for r in result:
                print(f"    {r}")
            print(f" Expected:")
            for e in expected:
                print(f"    {e}")

            for i, (r, e) in enumerate(zip(result, expected)):
                print(f"    comparing {i}: {r == e}")
                assert r == e
                
            print("Dashed Line Check:")
            is_correct_header = lambda line: re.match(r"^\+(-+)\+(-+)\+(-+)\+$", line)
            print(f"      start_header_line: {bool(is_correct_header(start_header_line))}, end_header_line: {bool(is_correct_header(end_header_line))}, end_line: {bool(is_correct_header(end_line))}")
            assert is_correct_header(start_header_line) and is_correct_header(end_header_line) and is_correct_header(end_line)
        else:
            # attach prefix to expected
            if query != "exit;":
                expected = PREFIX + expected
            
            print(f"Test Name: {test_name}")
            print(f"    Query: {query}")
            print(f"   Output: {result}")
            print(f" Expected: {expected}")
            print()
            assert result == expected
            
        print(f"--------Test Case {i + 1} End--------")

            
    return True
    
def main(argv, args):
    global PREFIX
    if args.id:
        PREFIX = f"DB_{args.id}> "
    else:
        print("\nWARNING: Please set your student id. e.g. --id 1234-56789\n")
        PREFIX = f"DB_{DEFAULT_ID}> "

    success = setup()
    if not success:
        return
    print("--------Test Start-------\n")
    success = test(test_cases)

    if success:
        print("--------All test cases passed.-------\n")


if __name__ == "__main__":
    args = parser.parse_args()
    main(sys.argv, args)