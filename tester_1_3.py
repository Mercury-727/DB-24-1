'''
SNU Database 2024 spring Project 1-3
made by Jeonghun Park
'''

test_cases = [
    {
        "query": """create table students (
            id char (10) not null,
            name char (10),
            primary key (id)
        );""",
        "expected": "'students' table is created",
        "test_name": "create table students",
    },
    {
        "query": """create table lectures (
            id int not null,
            name char (10),
            capacity int,
            primary key (id)
        );""",
        "expected": "'lectures' table is created",
        "test_name": "create table lectures",
    },
    {
        "query": """create table ref (
            id int,
            foreign key (id) references lectures (id)
        );""",
        "expected": "'ref' table is created",
        "test_name": "create table ref",
    },
    {
        "query": """create table apply (
            s_id char (10) not null,
            l_id int not null,
            apply_date date,
            primary key (s_id, l_id),
            foreign key (s_id) references students (id),
            foreign key (l_id) references lectures (id)
        );""",
        "expected": "'apply' table is created",
        "test_name": "create table apply",
    },
    {
        "query": "select * from students;",
        "expected": "ID NAME",
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select all from students",
    },
    {
        "query": "insert into students values ('20180001', 'Alice');",
        "expected": "1 row inserted",
        "test_name": "insert into students",
    },
    {
        "query": "insert into students values ('extremlylonglongverylongalotofcharactersId', 'Bob');",
        "expected": "1 row inserted",
        "test_name": "insert into students with long id",
    },
    {
        "query": "insert into students values ('20241234', 'veryveryveryverylonglonglonglongname');",
        "expected": "1 row inserted",
        "test_name": "insert into students with long name",
    },
    {
        "query": "insert into students (name, id) values ('James', '20221234');",
        "expected": "1 row inserted",
        "test_name": "insert into students with different column order",
    },
    {
        "query": "insert into students values ('19561234', null);",
        "expected": "1 row inserted",
        "test_name": "insert into students with null value",
    },
    {
        "query": "insert into students (id) values ('20141234');",
        "expected": "1 row inserted",
        "test_name": "insert into students with missing value, name column becomes null",
    },
    {
        "query": "insert into students values ('20001234', 'null');",
        "expected": "1 row inserted",
        "test_name": "insert into students with string 'null' value",
    },
    {
        "query": "select * from students;",
        "expected": """
        ID NAME
        20180001 Alice
        extremlylo Bob
        20241234 veryveryve
        20221234 James
        19561234 null
        20141234 null
        20001234 null
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "check if the characters are truncated",
    },
    {
        "query": "insert into non_exist_table values ('20180002', 'Bob');",
        "expected": "No such table",
        "test_name": "insert into non_exist_table",
    },
    {
        "query": "insert into students (id, name) values ('20181234');",
        "expected": "Insertion has failed: Types are not matched",
        "test_name": "insert into students with unmatched column count",
    },
    {
        "query": "insert into students (id, name) values ('20181234', 'asdf', '12345');",
        "expected": "Insertion has failed: Types are not matched",
        "test_name": "insert into students with unmatched column count",
    },
    {
        "query": "insert into students values ('20181234', 'asdf', '12345');",
        "expected": "Insertion has failed: Types are not matched",
        "test_name": "insert into students with unmatched column count (implicit assignment)",
    },
    {
        "query": "insert into students (id, name) values (20181234, 'James');",
        "expected": "Insertion has failed: Types are not matched",
        "test_name": "insert into students with unmatched type (explicit assignment, id column)",
    },
    {
        "query": "insert into students (id, name) values ('20181234', 12345);",
        "expected": "Insertion has failed: Types are not matched",
        "test_name": "insert into students with unmatched type (explicit assignment, name column)",
    },
    {
        "query": "insert into students values (20181234, 'James');",
        "expected": "Insertion has failed: Types are not matched",
        "test_name": "insert into students with unmatched type (implicit assignment, id column)",
    },
    {
        "query": "insert into students values ('20181234', 12345);",
        "expected": "Insertion has failed: Types are not matched",
        "test_name": "insert into students with unmatched type (implicit assignment, name column)",
    },
    {
        "query": "insert into students values (null, 'Hello');",
        "expected": "Insertion has failed: 'id' is not nullable",
        "test_name": "insert into students with null value to not null column",
    },
    {
        "query": "insert into students (id, name) values (null, 'Hello');",
        "expected": "Insertion has failed: 'id' is not nullable",
        "test_name": "insert into students with null value to not null column (explicit assignment)",
    },
    {
        "query": "insert into students (id, name, non_exist_col) values ('18239231', 'Hello', 'nonvalue');",
        "expected": "Insertion has failed: 'non_exist_col' does not exist",
        "test_name": "insert into students with non exist column",
    },
    {
        "query": "insert into students (id, non_exist_col) values ('18239231', 'nonvalue');",
        "expected": "Insertion has failed: 'non_exist_col' does not exist",
        "test_name": "insert into students with non exist column",
    },
    {
        "query": "insert into students (non_exist_col) values ('nonvalue');",
        "expected": "Insertion has failed: 'non_exist_col' does not exist",
        "test_name": "insert into students with non exist column",
    },
    {
        "query": "delete from students;",
        "expected": "7 row(s) deleted",
        "test_name": "delete all from students",
    },
    {
        "query": "select * from students;",
        "expected": "ID NAME",
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select all from students after delete",
    },
    {
        "query": """create table test_table (
            int_col int,
            char_col char (10),
            date_col date,
            primary key (int_col)    
        );""",  
        "expected": "'test_table' table is created",
    },
    {
        "query": "insert into test_table values (1001, 'char1', 2024-05-01);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1001, 'char1', 2024-05-01)",  
    },
    {
        "query": "insert into test_table values (1002, 'char2', 2024-05-02);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1002, 'char2', 2024-05-02)",  
    },
    {
        "query": "insert into test_table values (1003, 'char3', 2024-05-03);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1003, 'char3', 2024-05-03)",  
    },
    {
        "query": "insert into test_table values (1004, 'char4', 2024-05-04);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1004, 'char4', 2024-05-04)",  
    },
    {
        "query": "insert into test_table values (1005, 'char4', 2024-05-05);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1005, 'char4', 2024-05-05)",  
    },
    {
        "query": "insert to students values ('20180002', 'Bob');",
        "expected": "Syntax error",
        "test_name": "query syntax error",
    },
    
    {
        "query": "delete from test_table where int_col = 1001;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing int value",
    },
    {
        "query": "delete from test_table where int_col = 1000;",
        "expected": "0 row(s) deleted",
        "test_name": "delete with where clause not matching any row",
    },
    {
        "query": "delete from test_table where 'char2' = char_col;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing char value",
    },
    {
        "query": "delete from test_table where date_col = 2024-05-03;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing date value",
    },
    {
        "query": "select * from test_table;",
        "expected": """
        INT_COL CHAR_COL DATE_COL
        1004 char4 2024-05-04
        1005 char4 2024-05-05
        """,
        "optional_headers": ["TEST_TABLE.INT_COL TEST_TABLE.CHAR_COL TEST_TABLE.DATE_COL"],
        "table_name": ["test_table"],
        "test_name": "select all from test_table after delete",  
    },
    {
        "query": "delete from test_table where int_col > 1004;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing int value",
    },
    {
        "query": "delete from test_table where int_col >= 1004;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing int value",
    },
    {
        "query": "delete from test_table where int_col < 1004;",
        "expected": "0 row(s) deleted",
        "test_name": "delete with where clause comparing int value",
    },
    {
        "query": "delete from test_table where int_col <= 1004;",
        "expected": "0 row(s) deleted",
        "test_name": "delete with where clause comparing int value",
    },
    {
        "query": "delete from test_table where int_col != 1002;",
        "expected": "0 row(s) deleted",
        "test_name": "delete with where clause comparing int value",
    },
    {
        "query": "insert into test_table values (1001, 'char1', 2024-05-01);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1001, 'char1', 2024-05-01)",  
    },
    {
        "query": "insert into test_table values (1002, 'char2', 2024-05-02);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1002, 'char2', 2024-05-02)",  
    },
    {
        "query": "insert into test_table values (1003, 'char3', 2024-05-03);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1003, 'char3', 2024-05-03)",  
    },
    {
        "query": "insert into test_table values (1004, 'char4', 2024-05-04);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1004, 'char4', 2024-05-04)",  
    },
    {
        "query": "insert into test_table values (1005, 'char4', 2024-05-05);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1005, 'char4', 2024-05-05)",  
    },
    
    {
        "query": "delete from test_table where date_col > 2024-05-04;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing date value",
    },
    {
        "query": "delete from test_table where date_col >= 2024-05-04;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing date value",
    },
    {
        "query": "delete from test_table where date_col < 2024-05-02;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing date value",
    },
    {
        "query": "delete from test_table where date_col <= 2024-05-02;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing date value",
    },
    {
        "query": "delete from test_table where date_col != 2024-05-02;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing date value",
    },

    {
        "query": "insert into test_table values (1001, 'char1', 2024-05-01);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1001, 'char1', 2024-05-01)",  
    },
    {
        "query": "insert into test_table values (1002, 'char2', 2024-05-02);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1002, 'char2', 2024-05-02)",  
    },
    {
        "query": "insert into test_table values (1003, 'char3', null);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1003, 'char3', 2024-05-03)",  
    },
    {
        "query": "insert into test_table values (1004, 'char4', 2024-05-04);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1004, 'char4', 2024-05-04)",  
    },
    {
        "query": "insert into test_table values (1005, 'char4', 2024-05-05);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1005, 'char4', 2024-05-05)",  
    },

    {
        "query": "delete from test_table where char_col = 'char4';",
        "expected": "2 row(s) deleted",
        "test_name": "delete with where clause comparing char value, multiple rows",  
    },
    {
        "query": "delete from test_table where date_col is null;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause comparing null value",
    },
    {
        "query": "delete from test_table where date_col is not null;",
        "expected": "2 row(s) deleted",
        "test_name": "delete with where clause comparing not null value",
    },
    
    {
        "query": "insert into test_table values (1001, 'char1', 2024-05-01);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1001, 'char1', 2024-05-01)",  
    },
    {
        "query": "insert into test_table values (1002, 'char2', 2024-05-02);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1002, 'char2', 2024-05-02)",  
    },
    {
        "query": "insert into test_table values (1003, 'char3', null);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1003, 'char3', 2024-05-03)",  
    },
    {
        "query": "insert into test_table values (1004, 'char4', 2024-05-04);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1004, 'char4', 2024-05-04)",  
    },
    {
        "query": "insert into test_table values (1005, 'char4', 2024-05-05);",
        "expected": "1 row inserted",
        "test_name": "insert into test_table (1005, 'char4', 2024-05-05)",  
    },
    
    {
        "query": "delete from test_table where int_col = 1001 or int_col = 1002;",
        "expected": "2 row(s) deleted",
        "test_name": "delete with where clause using compound condition",
    },
    {
        "query": "delete from test_table where int_col = 1003 and char_col = 'char3';",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause using compound condition",
    },
    {
        "query": "delete from test_table where int_col = 1004 and date_col = 2024-05-23;",
        "expected": "0 row(s) deleted",
        "test_name": "delete with where clause using compound condition",
    },
    {
        "query": "delete from test_table where test_table.int_col = 1004;",
        "expected": "1 row(s) deleted",
        "test_name": "delete with where clause using table name",
    },
    
    {
        "query": "delete from test_table where int_col > '1234';",
        "expected": "Where clause trying to compare incomparable values",
        "test_name": "delete with where clause comparing incomparable values",
    },
    {
        "query": "delete from test_table where non_exit_table.id = 1001;",
        "expected": "Where clause trying to reference tables which are not specified",
        "test_name": "delete with where clause referencing non exist table",
    },
    {
        "query": "delete from test_table where non_exist_col = 1001;",
        "expected": "Where clause trying to reference non existing column",
        "test_name": "delete with where clause referencing non exist column",
    },
    {
        "query": "delete from test_table where test_table.non_exist_col = 1001;",
        "expected": "Where clause trying to reference non existing column",
        "test_name": "delete with where clause referencing non exist column",
    },
    {
        "query": "select * from test_table;",
        "expected": """
        INT_COL CHAR_COL DATE_COL
        1005 char4 2024-05-05
        """,
        "optional_headers": ["TEST_TABLE.INT_COL TEST_TABLE.CHAR_COL TEST_TABLE.DATE_COL"],
        "table_name": ["test_table"],
        "test_name": "select all from test_table after delete",
    },
    
    {
        "query": "insert into students values ('0001', 'Alice');",
        "expected": "1 row inserted",
        "test_name": "insert into students",
    },
    {
        "query": "insert into students values ('0002', 'Bob');",
        "expected": "1 row inserted",
        "test_name": "insert into students",
    },
    {
        "query": "insert into students values ('0003', 'Charlie');",
        "expected": "1 row inserted",
        "test_name": "insert into students",
    },
    {
        "query": "insert into lectures values (1, 'Math', 100);",
        "expected": "1 row inserted",
        "test_name": "insert into lectures",
    },
    {
        "query": "insert into lectures values (2, 'Database', 200);",
        "expected": "1 row inserted",
        "test_name": "insert into lectures",
    },
    {
        "query": "insert into lectures values (3, 'English', 50);",
        "expected": "1 row inserted",
        "test_name": "insert into lectures",
    },
    {
        "query": "insert into lectures values (4, 'Physics', null);",
        "expected": "1 row inserted",
        "test_name": "insert into lectures",  
    },
    {
        "query": "insert into ref values (1);",
        "expected": "1 row inserted",
        "test_name": "insert into ref",
    },
    {
        "query": "insert into apply values ('0001', 1, 2024-05-01);",
        "expected": "1 row inserted",
        "test_name": "insert into apply",
    },
    {
        "query": "insert into apply values ('0002', 2, 2024-05-02);",
        "expected": "1 row inserted",
        "test_name": "insert into apply",
    },
    {
        "query": "insert into apply values ('0003', 1, 2024-05-03);",
        "expected": "1 row inserted",
        "test_name": "insert into apply",
    },
    {
        "query": "select * from students;",
        "expected": """
        ID NAME
        0001 Alice
        0002 Bob
        0003 Charlie
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select all from students",
    },
    {
        "query": "select * from lectures;",
        "expected": """
        ID NAME CAPACITY
        1 Math 100
        2 Database 200
        3 English 50
        4 Physics null
        """,
        "optional_headers": ["LECTURES.ID LECTURES.NAME LECTURES.CAPACITY"],
        "table_name": ["lectures"],
        "test_name": "select all from lectures",
    },
    {
        "query": "select * from ref;",
        "expected": """
        ID
        1
        """,
        "optional_headers": ["REF.ID"],
        "table_name": ["ref"],
        "test_name": "select all from ref",
    },
    {
        "query": "select * from apply;",
        "expected": """
        S_ID L_ID APPLY_DATE
        0001 1 2024-05-01
        0002 2 2024-05-02
        0003 1 2024-05-03
        """,
        "optional_headers": ["APPLY.S_ID APPLY.L_ID APPLY.APPLY_DATE"],
        "table_name": ["apply"],
        "test_name": "select all from apply",
    },
    {
        "query": "select * from students where id = '0001';",
        "expected": """
        ID NAME
        0001 Alice
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select with where clause",
    },
    {
        "query": "select * from students where id = '0002' and name = 'Bob';",
        "expected": """
        ID NAME
        0002 Bob
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select with where clause",
    },
    {
        "query": "select * from lectures where capacity is null;",
        "expected": """
        ID NAME CAPACITY
        4 Physics null
        """,
        "optional_headers": ["LECTURES.ID LECTURES.NAME LECTURES.CAPACITY"],
        "table_name": ["lectures"],
        "test_name": "select with where clause",
    },
    {
        "query": "select id, name from students where id = '0001' or id = '0002';",
        "expected": """
        ID NAME
        0001 Alice
        0002 Bob
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select specific columns with where clause",
    },
    {
        "query": "select students.id, students.name from students where students.id = '0001' or students.id = '0002';",
        "expected": """
        ID NAME
        0001 Alice
        0002 Bob
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select specific columns with where clause, using table name",
    },
    {
        "query": "select * from lectures, ref;",
        "expected": """
        ID NAME CAPACITY ID
        1 Math 100 1
        2 Database 200 1
        3 English 50 1
        4 Physics null 1
        """,
        "optional_headers": ["LECTURES.ID NAME CAPACITY REF.ID", "LECTURES.ID LECTURES.NAME LECTURES.CAPACITY REF.ID"],
        "table_name": ["lectures", "ref"],
        "test_name": "select with multiple tables",
    },
    {
        "query": "select * from students, apply where students.id = apply.s_id;",
        "expected": """
        ID NAME S_ID L_ID APPLY_DATE
        0001 Alice 0001 1 2024-05-01
        0002 Bob 0002 2 2024-05-02
        0003 Charlie 0003 1 2024-05-03
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME APPLY.S_ID APPLY.L_ID APPLY.APPLY_DATE"],
        "table_name": ["students", "apply"],
        "test_name": "select with multiple tables and where clause",
    },
    {
        "query": "select lectures.id, name from lectures, ref where lectures.id = ref.id;",
        "expected": """
        ID NAME
        1 Math
        """,
        "optional_headers": ["LECTURES.ID LECTURES.NAME", "LECTURES.ID NAME"],
        "table_name": ["lectures", "ref"],
        "test_name": "select with multiple tables, specific columns and where clause",  
    },
    {
        "query": "select * from students where 1 = 1;",
        "expected": """
        ID NAME
        0001 Alice
        0002 Bob
        0003 Charlie
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select with where clause always true",
    },
    {
        "query": """select * from students where "1" = '1';""",
        "expected": """
        ID NAME
        0001 Alice
        0002 Bob
        0003 Charlie
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME"],
        "table_name": ["students"],
        "test_name": "select with using both double quote and single quote",  
    },
    {
        "query": "create table empty_table (id int);",
        "expected": "'empty_table' table is created",
        "test_name": "create table empty_table",  
    },
    {
        "query": "select * from students, empty_table;",
        "expected": """
        ID NAME ID
        """,
        "optional_headers": ["STUDENTS.ID STUDENTS.NAME EMPTY_TABLE.ID", "STUDENTS.ID NAME EMPTY_TABLE.ID"],
        "table_name": ["students", "empty_table"],
        "test_name": "join with empty table",
    },
    {
        "query": "select * from students where id = 1234;",
        "expected": "Where clause trying to compare incomparable values",
        "test_name": "select with where clause comparing incomparable values",
    },
    {
        "query": "select * from students where non_exist_table.id = '0001';",
        "expected": "Where clause trying to reference tables which are not specified",
        "test_name": "select with where clause referencing non exist table",
    },
    {
        "query": "select * from students where non_exist_col = '0001';",
        "expected": "Where clause trying to reference non existing column",
        "test_name": "select with where clause referencing non exist column",
    },
    {
        "query": "select * from students where students.non_exist_col = '0001';",
        "expected": "Where clause trying to reference non existing column",
        "test_name": "select with where clause referencing non exist column",
    },
    {
        "query": "select * from students, apply, lectures where name = 'abcd';",
        "expected": "Where clause contains ambiguous reference",
        "test_name": "select with ambiguous reference",
    },
    {
        "query": "select * from students, apply, lectures where id = s_id and l_id = id;",
        "expected": "Where clause contains ambiguous reference",
        "test_name": "select with ambiguous reference",
    },
    {
        "query": "select * from non_exist_table;",
        "expected": "Selection has failed: 'non_exist_table' does not exist",
        "test_name": "select from non exist table",
    },
    {
        "query": "select non_exist_col from students;",
        "expected": "Selection has failed: fail to resolve 'non_exist_col'",
        "test_name": "select non exist column",
    },
    {
        "query": "select name from students, lectures;",
        "expected": "Selection has failed: fail to resolve 'name'",
        "test_name": "select ambiguous column",  
    },
    {
        "query": "select abc.name from students;",
        "expected": "Selection has failed: fail to resolve 'abc.name'",
        "test_name": "select ambiguous column",  
    },
    {
        "query": "",
        "expected": "",
        "test_name": "",
        "optional": True,
        # TODO add test cases for optional specification which test key constraints
    },
    {   
        "query": "exit;",
        "expected": "",
        "test_name": "exit"
    }
]

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
    print("Note: There can be multiple valid headers for select query. To see all valid headers, check the 'optional_headers' field in test cases.")
    required_db_files = ["example.db", "example2.db"] # FIXME: add your required db files here
    
    user_input = input("Do you want to remove all db files in this directory before starting test? (y/n): ").strip()
    
    optional = input("Do you want to run optional test cases which test key constraints? (y/n): ").strip()
    if optional.lower() == "y":
        optional = True
    else:
        optional = False
    
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
        return (True, optional)
    else:
        print("Cannot start test without removing db files. Backup your db files and try again.")
        return (False, optional)

def process_lines(lines):
    lines = lines.strip().split("\n")
    lines = [line.strip() for line in lines]
    lines = [re.sub(r"\s+", " ", line) for line in lines] # remove multiple spaces
    return lines
    
def print_output(test_name, query, result, expected):
    print(f"Test Name: {test_name}")
    print(f"    Query: {query}")
    print(f"   Output:")
    for r in result:
        print(f"    {r}")
    print(f" Expected:")
    for e in expected:
        print(f"    {e}")

def compare_lines(result, expected):
    for k, (r, e) in enumerate(zip(result, expected)):
        print(f"    comparing {k}: {r == e}")
        assert r == e

def is_dashed_line(line):
    return bool(re.match(r"^-+$", line))

def is_correct_header(line):
    return bool(re.match(r"^(\+(-+))+\+$", line))

def query_matched(query, query_types):
    return any([query.lower().startswith(q) for q in query_types])

def test(test_cases, optional=False):
    test_cases_to_test = []
    if optional:
        test_cases_to_test = test_cases[:]
    else:
        test_cases_to_test = [test_case for test_case in test_cases if not test_case.get("optional", False)]
    for i, test_case in enumerate(test_cases_to_test):
        popen = subprocess.Popen(["python3", "run.py"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        query = re.sub(r"\s+", " ", " ".join(test_case["query"].split("\n")))

        expected = test_case["expected"]
        test_name = test_case.get("test_name", None)
        
        std_out, std_err = popen.communicate(input=(query + "\n").encode("utf-8"))
        std_out, std_err = (std_out).decode("utf-8"), (std_err).decode("utf-8")

        if std_err:
            if "EOFError" in std_err:
                # discard eof error
                pass
            else:
                print("Error occurred while running run.py")
                print(std_err)
                return False

        # remove prefix
        result = std_out[len(PREFIX):].strip()
        last_prefix_idx = result.rfind(PREFIX.strip())
        result = result[:last_prefix_idx].strip()
        # print(query, result)
        
        print(f"--------Test Case {i + 1} Start--------")
        describe_quries = ["explain", "desc", "describe"]
        show_query = ["show"]
        select_query = ["select"]

        print("     Note: Ignore dashed lines, white spaces and the order when evaluating desc/explain/describe/select/show query.")
        print("     Note: There can be multiple valid headers for select query. To see all valid headers, check the 'optional_headers' field in test cases.")
        if query_matched(query, describe_quries) and expected != "No such table":
            # describe
            expected = process_lines(expected)
            expected = [expected[0]] + sorted(expected[1:])
            
            result = process_lines(result)
            start_line, end_line = result[0], result[-1]
            result = result[1:-1]
            result = [result[0]] + sorted(result[1:])
            
            print_output(test_name, query, result, expected)

            compare_lines(result, expected)
            
            print("Dashed Line Check:")
            print(f"      start_line: {is_dashed_line(start_line)}, end_line: {is_dashed_line(end_line)}")
            assert is_dashed_line(start_line) and is_dashed_line(end_line)
        elif query_matched(query, show_query):
            # show
            expected = process_lines(expected)
            expected.sort()
            
            result = process_lines(result)
            start_line, end_line = result[0], result[-1]
            result = result[1:-1]
            result.sort()
            
            print_output(test_name, query, result, expected)
            compare_lines(result, expected)
            
            print("Dashed Line Check:")
            print(f"      start_line: {is_dashed_line(start_line)}, end_line: {is_dashed_line(end_line)}")
            assert is_dashed_line(start_line) and is_dashed_line(end_line)
        elif query_matched(query, select_query) and not expected.startswith("Selection has failed") and not expected.startswith("Where clause"):
            # select
            expected = process_lines(expected)
            expected = [expected[0]] + sorted(expected[1:])
            
            result = result.strip().split("\n")
            result = [re.sub(r"\|", " ", line) for line in result] # remove | character
            result = [line.strip() for line in result]
            start_header_line, end_header_line, end_line = result[0], result[2], result[-1]
            result = [result[1]] + result[3:-1]
            result = [re.sub(r"\s+", " ", line) for line in result] # remove multiple spaces
            result = [result[0]] + sorted(result[1:])
            
            print_output(test_name, query, result, expected)

            # compare header
            candidate_headers = test_case.get("optional_headers", [])
            candidate_headers.append(expected[0])
            print("    comparing header:", (result[0] in candidate_headers))
            assert (result[0] in candidate_headers)
            
            print("    comparing data:")
            compare_lines(result[1:], expected[1:])
                
            print("Dashed Line Check:")
            print(f"      start_header_line: {is_correct_header(start_header_line)}, end_header_line: {is_correct_header(end_header_line)}, end_line: {is_correct_header(end_line)}")
            assert is_correct_header(start_header_line) and is_correct_header(end_header_line) and is_correct_header(end_line)
        else:
            # if query is sequence, split query
            query = query.split(";")
            query = [q.strip() + ";" for q in query if q.strip()]
            query = [q for q in query if q]
            query = " ".join(query)
            
            expected = expected.split("\n")
            e = []
            for k in range(len(expected)):
                exp = expected[k].strip()
                if exp:
                    e.append(exp if exp.startswith(PREFIX) else PREFIX + exp)
            expected = e
            result = [r.strip() for r in result.split("\n") if r.strip()]

            
            print_output(test_name, query, result, expected)
            compare_lines(result, expected)
            
        print(f"--------Test Case {i + 1} End--------")

    return True
    
def main(argv, args):
    global PREFIX
    if args.id:
        PREFIX = f"DB_{args.id}> "
    else:
        print("\nWARNING: Please set your student id. e.g. --id 1234-56789\n")
        PREFIX = f"DB_{DEFAULT_ID}> "

    (success, optional) = setup()
    if not success:
        return
    print("--------Test Start-------\n")
    success = test(test_cases, optional=optional)

    if success:
        print("--------All test cases passed.-------\n")


if __name__ == "__main__":
    args = parser.parse_args()
    main(sys.argv, args)