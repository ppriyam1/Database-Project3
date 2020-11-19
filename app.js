var http = require('http');
var url = require('url');
var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
var url1 = "mongodb://localhost:27017/";

MongoClient.connect(url1, {
  useUnifiedTopology: true
}, function(err, db) {
  var collectionTweet = db.db('twitterDB').collection('tweets');
  var collectionuser = db.db('twitterDB').collection('users');
  var amount;
  if (err) throw err;

  var max = 0;
  var sec = 0;
  var thi = 0;
  var mtags = 0;
  var stags = 0;
  var ttags = 0;
  var mtl = [0, 0, 0];
  var mt = [];
  var stl = [0, 0, 0];
  var st = [];
  var ttl = [0, 0, 0];
  var tt = [];

  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async function getUser() {
    var userArray = [];
    collectionuser.find({}).toArray(
      function(err, result) {
        if (err) {
          console.log(err);
          return err;
        }
        result.forEach((item, i) => {
          userArray.push(item.username);
        });
      }
    )
    await sleep(100);
    return userArray;
  }

  /* =================== simpleQuery1 TWEETS BASED ON SPECIFIC DATE =================== */

  function getTweets(date, cb) {
    var tweets = null;
    var count = 1;
    var num;
    var dbDate;
    collectionTweet.find({}).toArray(
      function(err, result) {
        if (err) {
          console.log(err);
          return err;
        }
        result.forEach((item, i) => {
          num = item.time;
          dbDate = num.slice(0, 10);
          if (date == dbDate) {
            console.log(item.time + "\t tweet = " + item.tweetID)
          }
          count++;
        });
        if (cb) {
          console.log("\n");
          cb();
        }
      });
  }
  //getTweets("2020-11-05");

  /* =================== simpleQuery2 FIND THE AMOUNT OF USERS =================== */

  async function simpleQuery2(cb) {
    collectionuser.find({}).count(function(err, result) {
      console.log('amount of users:');
      if (err) {
        console.log('Error: ' + err);
        return;
      }
      amount = result;
      console.log(amount);
      console.log("");
      //await sleep(10)
      if (cb) {
        //console.log("\n");
        cb();
      }
    });
  }
  //simpleQuery2();

  /* =================== simpleQuery3 TOTAL NUMBER OF TWEETS COUNT BY LOCATION =================== */

  function getTweetCountByLoc(cb) {
    var tweets = null;
    var count = 1;
    collectionTweet.aggregate([{
      $group: {
        _id: "$location",
        tweetsCount: {
          $sum: 1
        }
      }
    }]).toArray(
      function(err, result) {
        if (err) {
          console.log(err);
          return err;
        }
        console.log(result)
        if (cb) {
          console.log("\n");
          cb();
        }
      });
  }
  //getTweetCountByLoc();

  /* =================== simpleQuery4 AMOUNT OF USERS WITH NO TWEETS =================== */

  function simpleQuery4(cb) {
    collectionTweet.aggregate({
      $group: {
        _id: "$user"
      }
    }, {
      $group: {
        _id: 1,
        num: {
          $sum: 1
        }
      }
    }).toArray(function(err, result) {
      console.log('amount of users with no tweets:');
      if (err) {
        console.log('Error: ' + err);
        return;
      }
      var num = 0;
      result.forEach((item, i) => {
        num += 1;
      });
      console.log(amount - num);
      console.log("");
      if (cb) {
        cb();
      }
    });
  }
  //simpleQuery4();

  /* =================== complexQuery1 HOMEPAGE OF A USER =================== */

  async function getHomePage(user, cb) {
    var followings = null;
    collectionuser.find({
      "username": user
    }).toArray(
      function(err, result) {
        if (err) {
          console.log(err);
          return err;
        }
        followings = result[0].following;
      });
    await sleep(100)
    var tempArray = []
    followings.forEach((user, i) => {
      tempArray.push({
        "user": user
      })
    });
    collectionTweet.find({
      $or: tempArray
    }).sort({
      time: -1
    }).limit(10).toArray(
      function(err, result) {
        if (err) {
          console.log(err);
          return err;
        }
        //console.log("tweets for user " + user)
        for (let index in result) {
          console.log("timeStamp = " + result[index].time + " from user: " + result[index].user + " tweetdId = " + result[index].tweetID);
        };
        if (cb) {
          cb();
        }
      });
  }
  //getHomePage("barefoot_exec");


  /* =================== complexQuery2 TWEETS BASED ON HASHTAGS =================== */

  //complexQuery2 HELPER FUNCTION
  function query2(mtags, stags, ttags) {
    collectionTweet.find({
      $or: [{
        hashtags: {
          $elemMatch: {
            $eq: mtags
          }
        }
      }, {
        hashtags: {
          $elemMatch: {
            $eq: stags
          }
        }
      }, {
        hashtags: {
          $elemMatch: {
            $eq: ttags
          }
        }
      }]
    }).toArray(function(err, result) {
      if (err) {
        console.log('Error: ' + err);
        return;
      }
      result.forEach((item, i) => {
        if (item.hashtags.indexOf(mtags) != -1) {
          if (item.likes > mtl[0]) {
            mtl[0] = item.likes;
            mt[0] = item.description;
          } else if (item.likes > mtl[1]) {
            mtl[1] = item.likes;
            mt[1] = item.description;
          } else if (item.likes > mtl[2]) {
            mtl[2] = item.likes;
            mt[2] = item.description;
          }
        } else if (item.hashtags.indexOf(stags) != -1) {
          if (item.likes > stl[0]) {
            stl[0] = item.likes;
            st[0] = item.description;
          } else if (item.likes > stl[1]) {
            stl[1] = item.likes;
            st[1] = item.description;
          } else if (item.likes > stl[2]) {
            stl[2] = item.likes;
            st[2] = item.description;
          }
        } else if (item.hashtags.indexOf(ttags) != -1) {
          if (item.likes > ttl[0]) {
            ttl[0] = item.likes;
            tt[0] = item.description;
          } else if (item.likes > ttl[1]) {
            ttl[1] = item.likes;
            tt[1] = item.description;
          } else if (item.likes > ttl[2]) {
            ttl[2] = item.likes;
            tt[2] = item.description;
          }
        }
      });
      console.log("Top three hashtags and their top three liked tweets:")
      console.log("1. #" + mtags + " " + max);
      for (var i = 0; i < 3; i++) {
        console.log("(" + (i + 1) + ")" + ". tweet: " + mt[i]);
        console.log("likes: " + mtl[i]);
      }
      console.log("2. #" + stags + " " + sec);
      for (var i = 0; i < 3; i++) {
        console.log("(" + (i + 1) + ")" + ". tweet: " + st[i]);
        console.log("likes: " + stl[i]);
      }
      console.log("3. #" + ttags + " " + sec);
      for (var i = 0; i < 3; i++) {
        console.log("(" + (i + 1) + ")" + ". tweet: " + tt[i]);
        console.log("likes: " + ttl[i]);
      }
    });
  }
  // complexQuery2
  async function complexQuery2(cb){
  collectionTweet.aggregate({
    $unwind: "$hashtags"
  }, {
    $group: {
      _id: "$hashtags",
      num: {
        $sum: 1
      }
    }
  }, {
    $match: {
      num: {
        $gt: 1
      }
    }
  }).toArray(async function(err, result) {
    if (err) {
      console.log('Error: ' + err);
      return;
    }
    result.forEach((item, i) => {
      if (item._id != null) {
        if (item.num > max) {
          max = item.num;
          mtags = item._id;
        } else if (item.num > sec) {
          sec = item.num;
          stags = item._id;
        } else if (item.num > thi) {
          thi = item.num;
          ttags = item._id;
        }
      }
    });
    query2(mtags, stags, ttags);
    await sleep(100);
    if (cb) {
      cb();
    }
  });
}


  /* ===================== complexQuery3  Recommend users ======================== */

  function findDifference(array1, array2) {
    var result = [];
    for (var index = 0; index < array2.length; index++) {
      if (!(array1.includes(array2[index]))) {
        result.push(array2[index]);
      }
    }
    return result;
  }
  //Testing above function ---------
  // console.log(findDifference(['x','y','z'],['x','a','b','z'] ))
  // console.log(findDifference(['x','y','z'],['x','a'] ))
  async function getReccomendedUsers(recUser, cb) {
    var recommendedUser = [];
    collectionuser.find({}).toArray(
      async function(err, result) {
        if (err) {
          console.log(err);
          return err;
        }
        var userFollowing = [];
        //followings for recUser
        for (var index = 0; index < result.length; index++) {
          if (result[index].username == recUser) {
            userFollowing = result[index].following;
            break;
          }
        }
        // console.log(userFollowing);
        result.forEach((item, i) => {

          if (item.username != recUser) {
            var itemFollowing = item.following
            var diff = findDifference(userFollowing, itemFollowing)
            if ((itemFollowing.length - diff.length) > ((userFollowing.length * 2) / 3)) {
              recommendedUser.push({
                [item.username]: diff
              });
            }
          }
        });
        await sleep(100)
        if (cb) {
          cb();
        }
      });
    await sleep(100);
    console.log("\n Recommended users based on 66% match for user " + recUser);
    console.log(recommendedUser);
  }
  //getReccomendedUsers("FoxNews");


  /* =================== complexQuery4 AMOUNT OF TWEETS EVERY THREE HOURS =================== */

  function complexQuery4(cb) {
    collectionTweet.aggregate({
      $project: {
        hour: {
          $substr: ["$time", 11, 2]
        }
      }
    }, {
      $group: {
        _id: "$hour",
        num: {
          $sum: 1
        }
      }
    }, {
      $match: {
        num: {
          $gt: 1
        }
      }
    }).toArray(function(err, result) {
      if (err) {
        console.log('Error: ' + err);
        return;
      }
      var one = 0; //0:00-2:59
      var two = 0; //3:00-5:59
      var three = 0; //6:00-8:59
      var four = 0; //9:00-11:59
      var five = 0; //12:00-14:59
      var six = 0; //15:00-17:59
      var seven = 0; //18:00-20:59
      var eight = 0; //21:00-23:59
      result.forEach((item, i) => {
        if (item._id == "00" || item._id == "01" || item._id == "02") {
          one += item.num;
        } else if (item._id == "03" || item._id == "04" || item._id == "05") {
          two += item.num;
        } else if (item._id == "06" || item._id == "07" || item._id == "08") {
          three += item.num;
        } else if (item._id == "09" || item._id == "10" || item._id == "11") {
          four += item.num;
        } else if (item._id == "12" || item._id == "13" || item._id == "14") {
          five += item.num;
        } else if (item._id == "15" || item._id == "16" || item._id == "17") {
          six += item.num;
        } else if (item._id == "18" || item._id == "19" || item._id == "20") {
          seven += item.num;
        } else if (item._id == "21" || item._id == "22" || item._id == "23") {
          eight += item.num;
        }
      });
      console.log("\nAmount of tweets every 3 hours:");
      console.log("0:00-2:59");
      console.log(one);
      console.log("3:00-5:59");
      console.log(two);
      console.log("6:00-8:59");
      console.log(three);
      console.log("9:00-11:59");
      console.log(four);
      console.log("12:00-14:59");
      console.log(five);
      console.log("15:00-17:59");
      console.log(six);
      console.log("18:00-20:59");
      console.log(seven);
      console.log("21:00-23:59");
      console.log(eight);
      console.log("");

      if (cb) {
        cb();
      }
    });
  }
  //complexQuery4();



  /* ==================================== MENU DRIVEN ======================================= */

  const readline = require("readline");
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });


  function interface() {

    console.log("\nPlease choose an option from the below menu: \n");
    console.log("1. [simpleQuery1] TWEETS BASED ON SPECIFIC DATE");
    console.log("2. [simpleQuery2] FIND THE AMOUNT OF USERS");
    console.log("3. [simpleQuery3] TOTAL NUMBER OF TWEETS COUNT BY LOCATION");
    console.log("4. [simpleQuery4] AMOUNT OF USERS WITH NO TWEETS");
    console.log("5. [complexQuery1] HOMEPAGE OF A USER");
    console.log("6. [complexQuery2] TWEETS BASED ON HASHTAGS");
    console.log("7. [complexQuery3] RECOMMEND USERS");
    console.log("8. [complexQuery4] AMOUNT OF TWEETS EVERY THREE HOURS");
    console.log("9. QUIT");
    var username = []
    getUser().then(result => username = result)


    var option = rl.question("\nWhich option you like to select ?\nEnter = ", function(opt) {

      switch (opt) {
        case '1': {
          console.log("\n");
          console.log("Which date you want to find tweets for ? (Format: YYYY-MM-DD)");
          console.log("mininum date: 2020-08-25, maximum date: 2020-11-05\n");
          rl.question("Date: ", function(date) {
            getTweets(date, function() {
              interface();
            });
          });
        }
        break;

      case '2': {
        console.log("\n");
        simpleQuery2(function() {
          interface();
        });
      }
      break;

      case '3':
        getTweetCountByLoc(function() {
          interface();
        });

        break;

      case '4':
        simpleQuery4(function() {
          interface();
        });
        break;

      case '5': {
        console.log("Which user's home page you would like to retrive ?");
        for (var index in username) {
          console.log("user " + index + ". " + username[index]);
        }
        rl.question("\nPlease enter the index = ", function(name) {
          getHomePage(username[name], function() {
            interface();
          });
        });

        break;
      }
      case '6':
      complexQuery2(function() {
        interface();
      });
      break;

      case '7': {
        console.log("Which user you want recommendation for ?");
        for (var index in username) {
          console.log("user " + index + ". " + username[index]);
        }
        rl.question("\nPlease enter the index = ", function(name) {
          getReccomendedUsers(username[name], function() {
            interface();
          });
        });

      }

      break;

      case '8':
        complexQuery4(function() {
          interface();
        });
        break;

      case '9':
        process.exit(0);
        break;
      default:
        return;
      }
    });
    rl.on("close", function() {
      console.log("\nBYE BYE !!!");
      process.exit(0);
    });
  }
  interface();
  /* ==================================== MENU DRIVEN ======================================= */

});

http.createServer(function handler(req, res) {
  //res.writeHead(200, {'Content-Type': 'text/plain'});
  //res.end('Hello World\n');
  var pathname = url.parse(req.url).pathname;
  console.log("Request for " + pathname + " received.");
  fs.readFile(pathname.substr(1), function(err, data) {
    if (err) {
      console.log(err);
      // HTTP 404 : NOT FOUND
      // Content Type: text/html
      res.writeHead(404, {
        'Content-Type': 'text/html'
      });
    } else {
      // HTTP 200 : OK
      // Content Type: text/html
      res.writeHead(200, {
        'Content-Type': 'text/html'
      });

      res.write(data.toString());
    }
    res.end();
  });
}).listen(1337, '127.0.0.1');

//console.log('Server running at http://127.0.0.1:1337/');
