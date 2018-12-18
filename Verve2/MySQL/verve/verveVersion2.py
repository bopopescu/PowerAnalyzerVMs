import json
from webob import Request
import pymysql.cursors
import time
import configparser

#Verve Script Version 2
#Python version 3.4

#obtains verve server and SQL server values from config.ini file,
#establishes connection with verve server,
#obtains a JSON array with 8 sensor values,
#forwards the values to SQL server every second
#NOTE: table in SQL server MUST ALWAYS be named "verve"

#enhancements needed:
    #add retries and timeouts for connections to the server
        #e.g. if a connection cannot be established within X seconds...
    #quadratic fit for the 8 sensors
    #make code a bit cleaner  by splitting into more modules

config = configparser.ConfigParser()
config.read("config.ini")

def config_section(section):
    optionsDict = {}
    options = config.options(section)
    for option in options:
        try:
            optionsDict[option] = config.get(section, option)
        except:
            print("Error: exception on %s.", option)
            optionsDict[option] = None
    return optionsDict

def main():
    try:
        verve_IP = config_section('Verve')['ip'] #obtain verve IP through config.ini file
        host = config_section('SQL')['host'] #obtain SQL server values through config.ini file
        user = config_section('SQL')['user']
        password = config_section('SQL')['password']
        db = config_section('SQL')['db']
        charset = config_section('SQL')['charset']
    except:
        print("Error: config file is missing core values.")
        return

    print("Initializing sensor data logging and transfer to SQL server...")
    print("Exit command: Ctrl + c")    
    try:
        while True:
            try:
                req = Request.blank(verve_IP) #connect to verve server IP
                resp = req.get_response()
                data = json.loads(resp.body.decode()) #parse JSON that is received from verve
                vals = (data["sensor01"], data["sensor02"], data["sensor03"], data["sensor04"],
                        data["sensor05"], data["sensor06"], data["sensor07"], data["sensor08"])
                print(vals)
            except:
                print("Error: Connection to verve unsuccessful; invalid IP.")
                break

            try:
                connection = pymysql.connect(host = host, #connect to SQL server
                                             user = user,
                                             password = password,
                                             db = db,
                                             charset = charset,
                                             cursorclass = pymysql.cursors.DictCursor)
            except:
                print("Error: Connection to SQL server unsuccessful.")
                break

            try:
                with connection.cursor() as cursor: #insert data into SQL table
                    sql = "INSERT INTO verve (sensor1, sensor2, sensor3, sensor4, sensor5, sensor6, sensor7, sensor8) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, vals)
                connection.commit() #save changes to SQL table
            except:
                print("Error: Insertion into SQL table unsuccessful.")
                connection.close()
                break
            connection.close()
            time.sleep(1) #1 second intervals between iterations
    except KeyboardInterrupt:
        print("Exit command received. All operations successful.")

if __name__ == '__main__':
    main()
