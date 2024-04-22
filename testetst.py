import subprocess
import os

qry = "create table referencing_table ( id char(10), role_id int, primary key (id), foreign key (role_id) references role (id) ); exit;"
popen = subprocess.Popen(['python3', 'run.py'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

stdout, stderr = popen.communicate(input=qry.encode())
print(stdout, stderr)
stdout, stderr = (stdout.decode(), stderr.decode())

print(qry)
print(stdout)
print("stderr"  + stderr)

# Run os.popen with command 'python3 run.py < qry'
# command = f"python3 run.py < test.txt"
# print("command: ", command)
# output = os.popen(command).read()
# print(output)

