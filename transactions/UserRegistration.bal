package transactions;

import ballerina.data.sql;
import ballerina.log;

struct User {
    string name;
    string password;
    int age;
    string country;
}


sql:ClientConnector sqlConn = create sql:ClientConnector(sql:DB.MYSQL, "localhost", 3306,
                                                         "userDB", "root", "Mathematics", {maximumPoolSize:5});

function main (string[] args) {
    endpoint<sql:ClientConnector> userDB {
        sqlConn;
    }
    int ret1 = userDB.update("DROP TABLE IF EXISTS USERINFO", null);
    int ret2 = userDB.update("CREATE TABLE USERINFO(USERNAME VARCHAR(10), PASSWORD VARCHAR(20),
                                AGE INT, COUNTRY VARCHAR(255), PRIMARY KEY (USERNAME))", null);

    User u1 = {name:"Alice", password:"Alice123", age:20, country:"USA"};
    User u2 = {name:"Bob", password:"bob123", age:21, country:"UK"};
    User[] userArr = [u1, u2];
    transaction with retries(0) {
        addUsers(userArr);
        // Expected Results
        log:printInfo("'Alice' and 'Bob' have succesfully registered");
        log:printInfo("Transaction committed");
    } failed {
        log:printError("Transaction failed");
    }
    log:printInfo("Registered users: " + getUsers());
    log:printInfo("Expected Results: You should see 'Alice' and 'Bob'\n");

    User u3 = {name:"Charles", password:"Charles123", age:25, country:"India"};
    User u4 = {name:"Alice", password:"AliceNew123", age:32, country:"Sri Lanka"};
    User[] userArr2 = [u3, u4];
    try {
        transaction with retries(0)  {
            addUsers(userArr2);
            log:printInfo("Transaction committed");
        } failed {
            log:printError("Transaction failed");
        }
    } catch(error e) {
        log:printInfo("Above error occured as expected: username 'Alice' is already taken");
    }
    log:printInfo("Registered users: " + getUsers() + "\n" +
                  "Expected Results: You shouldn't see 'charles'. " +
                  "Attempt to reuse username 'Alice' is a DB constraint violation. " +
                  "Therefore, Charles was rolled back in the same TX\n");

    User u5 = {name:"Dias", password:"Dias123", age:24, country:"Sri Lanka"};
    User u6 = {name:"UserWhoLovesCats", password:"ABC123", age:27, country:"India"};
    User[] userArr3 = [u6];
    try {
        transaction with retries(0) {
            addUsers(userArr3);
            log:printInfo("Transaction committed");
        } failed {
            log:printError("Transaction failed");
        }
    } catch(error e) {
        log:printInfo("Above error occured as expected: username is too big (Atmost 10 characters)");
    }
    log:printInfo("Registered users: " + getUsers() + "\n" +
                  "Expected Results: You shouldn't see 'Dias' and 'UserWhoLovesCats'. " +
                  "'UserWhoLovesCats' violated DB constraints, and 'Dias' was rolled back in the same TX\n");
    userDB.close();
}

function addUsers(User[] users) {
    endpoint<sql:ClientConnector> userDB {
        sqlConn;
    }
    int numOfUsers = lengthof users;
    int i;

    while(i < numOfUsers) {
        sql:Parameter para1 = {sqlType:sql:Type.VARCHAR, value:users[i].name};
        sql:Parameter para2 = {sqlType:sql:Type.VARCHAR, value:users[i].password};
        sql:Parameter para3 = {sqlType:sql:Type.INTEGER, value:users[i].age};
        sql:Parameter para4 = {sqlType:sql:Type.VARCHAR, value:users[i].country};
        sql:Parameter[] params = [para1, para2, para3, para4];
        int count = userDB.update("INSERT INTO USERINFO VALUES (?, ?, ?, ?)", params);
        i = i + 1;
    }
}

function getUsers()(string registeredUsers) {
    endpoint<sql:ClientConnector> userDB {
        sqlConn;
    }
    datatable dt = userDB.select("SELECT USERNAME FROM USERINFO", null, null);
    var dtJson, _ = <json> dt;
    registeredUsers = dtJson.toString();
    return;
}
