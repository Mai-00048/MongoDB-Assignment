//Student_No: 2840080

//-----------------------------------AssignmentB4-----------------------------\\
//Question 1
db.movies.aggregate([
    {$match:{actors:"Natalie Portman"}},//match for movies with the actors Natalie Portman
    {$unwind:"$actors"},//deconstruct the actors array into individual elements   
    {$group:{_id:"$actors",count:{$sum:1},year:{$push:"$year"}}},//grtoup the data
    {$match:{"count":{"$gt":1}}},//match for actors who have work more than one movies
    {$group:{_id:0,actors:{$push:{Actor_Name:"$_id",Count_of_Movies:"$count",Years_of_work: "$year"}}}}
]).pretty()//pretty format


//Question 2
db.movies.aggregate([
    {"$unwind":"$actors"},//deconstruct the actors array into individual elements
    {"$group":{"_id":{"movieType":"$genres","actorName":"$actors"}}},//groping
    {"$group":{"_id":"$_id.actorName","movie":{"$push":{"genres":"$_id.movieType","total":{"$sum":{"$size":"$_id.movieType"}}}}}},
    {$sort:{"movie.total":-1}},//Sort the total of movies in decreasing order 
    {$limit:4}//limit the result in 4 actors
]).pretty()//pretty format


//Question 3
db.movies.aggregate([
    {"$unwind": "$actors"}, //deconstruct the actors array into individual elements
    {"$group": {"_id": "$actors", "year": {"$addToSet": "$year"}}},//groping
    {"$project": {"Actor_Name": "$_id","MovieYears": {"$setUnion": ["$year"]},"_id": 0 }},
    {"$group": {"_id": "$Actor_Name","movie_Year": { "$push":{"years":   "$MovieYears","Numbers_of_Years":{$size: "$MovieYears"},"Start_Year":{$min: "$MovieYears"},"End_Year":{$max: "$MovieYears"}}}}},
    {$sort:{"movie_Year.Numbers_of_Years": -1}},//Sort the number years of movies in decreasing order 
    {$limit:4}//limit the result in 4 actors
]).pretty()//pretty format


//Question 4
db.movies.aggregate([
   { $match: {"metacritic": {"$exists": true}}},  
   {"$unwind": "$director"},
   {"$group": {"_id": "$director","Movies": {"$addToSet": "$title"},"Metacritic": { "$addToSet": "$metacritic"}}},
   {"$project": {"Director Name": "$_id","Number Of Movies": {$size: "$Movies"},"Average Metacritic Score": {$avg: "$Metacritic"},"_id": 0}},
   { $match: { "Number Of Movies": { $gt: 3}}},
   { $sort: { "Average Metacritic Score": -1}},
   {$limit: 4}
]).pretty()//pretty format

//Question 5
db.movies.aggregate([
   {$match: //match for movies with the genres crime and Adventure
   {"genres": 
     {$in: ["Crime", "Adventure"]}
   }},   
   {"$project"://selected title, year, tomato to get displayed 
     {"Movie Name": "$title", "Movie Year": "$year", "viewers reviews": "$tomato.userReviews", "specialised critics reviews ": "$tomato.reviews", "Total reviews": 
       {"$add":[
                "$tomato.userReviews","$tomato.reviews"
	    ]},
       "awards wins": "$awards.wins","awards nominations ": "$awards.nominations","Total awards": 
       {"$add":[
                "$awards.wins", "$awards.nominations"
        ]},"_id": 0}},//hide the _id    
  {$sort: {"Total reviews": -1}},//Sort the Total reviews in decreasing order
  {$limit: 5} //limit the result in 5 actors
]).pretty()//pretty format