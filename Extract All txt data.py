import os
import datetime

path = f'D:\\ActiveWork\\mda-pxmgt-rating\\environments\\common\\data\\view\\presentation_layer\\' 

files_content = ''
print('start at '+ str(datetime.datetime.now()))
for filename in filter(lambda p: p.endswith("sql"), os.listdir(path)):
    filepath = os.path.join(path, filename)
    
    with open(filepath, mode='r') as f:
        
        files_content += f.read()
#print(files_content)


with open(f"D:\\ActiveWork\\Extract all text run at "+ "{:%d-%m-%Y %H~%M~%S}".format(datetime.datetime.now()) + ".txt", "a") as f:
    f.write(files_content)
    #date(year, month, day)