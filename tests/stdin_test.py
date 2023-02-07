import time 
from subprocess import Popen, PIPE
p = Popen(["java", "-jar", "ISAToolkit.jar"], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
p.stdin.write(b"2\r\n")
time.sleep(1)
p.stdin.write(b"1\r\n")
time.sleep(1)
p.stdin.write(b"D:\\log\r\n")
res, err =p.communicate()
print(res)
print(err)