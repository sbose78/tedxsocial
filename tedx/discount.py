__author__ = 'sbose'

import string,random,os
def generate_random_password(length=20):
    chars = string.ascii_letters + string.digits
    random.seed = (os.urandom(1024))
    return ''.join(random.choice(chars) for i in range(length))

def write_to_file(code):
    f = open("tedxdiscouts.txt","w+")
    f.write("code\n")
    f.flush()
    f.close()

if __name__ == '__main__':
    f = open("tedxdiscouts.txt","w+")
    for i in range(1,101):
        code = str(generate_random_password(5)).upper()
        f.write(str(i)+",TEDXBLR"+code+"\n")
    f.flush()
    f.close()
