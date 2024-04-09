from lark import Lark, Transformer
import os

with open('grammar.lark') as file:
    sql_parser = Lark(file.read(),start="command",lexer="basic")

id = '2020-13624'
prompt = "DB_"+id+'> '

#각 query에 해당하는 것에 대한 request를 받았다는 출력을 하는 메소드
class MyTransformer(Transformer):
    def __init__(self, prompt):
       #super().__init__()
       pass
       self.prompt = prompt
    def create_table_query(self, items):
        print(prompt+"'CREATE TABLE' requested")
    def drop_table_query(self, items):
        print(prompt+"'DROP TABLE' requested")
    def select_query(self,items):
        print(prompt+"'SELECT' requested")
    def insert_query(self, items):
        print(prompt+"'INSERT' requested")
        print("나는 자연인이다",items,"나는 자연인이다")
    def delete_query(self, items):
        print(prompt+"'DELETE' requested")
    def update_query(self, items):
        print(prompt+"'UPDATE' requested")
    def explain_query(self,items):
        print(prompt+"'EXPLAIN' requested")
    def describe_query(self,items):
        print(prompt+"'DESCRIBE' requested")
    def desc_query(self,items):
        print(prompt+"'DESC' requested")
    def show_tables_query(self,items):
        print(prompt+"'SHOW TABLES' requested")
    def EXIT(self,items):
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
    else: tmp = input(prompt)
    input_str += tmp
    #';'으로 끝나지 않은 경우
    if(not input_str.endswith(';') or input_str == ''):
        flag=True
        continue
    #';'으로 끝났을 경우
    if input_str.endswith(';'):
        #받은 input_str을 ;에 대해 split한다.
        query_list = [s.strip() for s in input_str.split(';') if s.strip()]
        print(query_list)
        for q in query_list:
            #query_list element에 다시 ;을 붙여주고 transform에 넘겨준다.
            try:
                output = sql_parser.parse(q+';')
                #print(output,'\n')
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
                     