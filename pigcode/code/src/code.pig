MovieFile = LOAD '/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int, Title:chararray, Genres:chararray);
RatingFile = LOAD '/Spring-2016-input/ratings.dat' using PigStorage(':') as (UserID:int, MovieID:int, Rating:int, Timestamp:chararray);
UserFile = LOAD '/Spring-2016-input/users.dat' using PigStorage(':') as (UserID:int, Gender:chararray, Age:int, Occupation:int, Zipcode:chararray);

feasibleUser = filter UserFile by Gender == 'M' and Age > 20 and Age < 40 and Zipcode matches '7.*';

--find lowest rating comedy & drama
ComedyDrama = filter MovieFile by Genres matches '.*Comedy.*' and Genres matches '.*Drama.*';

RatingsCD = join RatingFile by MovieID, ComedyDrama by MovieID;

averageRating = foreach (group RatingsCD by RatingFile::MovieID)
    generate group as MovieID, AVG(RatingsCD.Rating) as Rating;

--lowestRating = limit (order averageRating by Rating ASC) 1;
lowestRate = foreach (group averageRating all) generate MIN(averageRating.Rating) as rate;
feasibleMovie = filter averageRating by Rating == lowestRate.rate;

--
--(4486,133,1,965013057,133,1.0)
--(1470,1115,1,974915361,1115,1.0)
userWhoRated = foreach (join RatingFile by MovieID, feasibleMovie by MovieID) generate RatingFile::UserID as UserID;
resultUserID = foreach (join userWhoRated by UserID, feasibleUser by UserID) generate feasibleUser::UserID as UserID;
dump resultUserID


--############# Ques 2 ##################
groupRes = cogroup RatingFile by MovieID, MovieFile by MovieID;
limitRes = limit groupRes 5;
dump limitRes


--############# Ques 3 ##################

FORMAT_GENRE
