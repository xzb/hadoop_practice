MovieFile = LOAD '/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int, Title:chararray, Genres:chararray);
RatingFile = LOAD '/Spring-2016-input/ratings.dat' using PigStorage(':') as (UserID:int, MovieID:int, Rating:int, Timestamp:chararray);
UserFile = LOAD '/Spring-2016-input/users.dat' using PigStorage(':') as (UserID:int, Gender:chararray, Age:int, Occupation:int, Zipcode:chararray);

feasibleUser = filter UserFile by Gender == 'F' and Age > 20 and Age < 35 and Zipcode matches '1.*';

--find lowest rating action & war movies
ActionWar = filter MovieFile by Genres matches '.*Action.*' and Genres matches '.*War.*';

RatingsCD = join RatingFile by MovieID, ActionWar by MovieID;

averageRating = foreach (group RatingsCD by RatingFile::MovieID)
    generate group as MovieID, AVG(RatingsCD.Rating) as Rating;

--lowestRating = limit (order averageRating by Rating ASC) 1;
lowestRate = foreach (group averageRating all) generate MIN(averageRating.Rating) as rate;
feasibleMovie = filter averageRating by Rating == lowestRate.rate;

--
userWhoRated = foreach (join RatingFile by MovieID, feasibleMovie by MovieID) generate RatingFile::UserID as UserID;
resultUserID = foreach (join userWhoRated by UserID, feasibleUser by UserID) generate feasibleUser::UserID as UserID;
dump resultUserID

--(673)
--(1010)
--(1835)
--(2931)


--############# Ques 2 ##################
MovieFile = LOAD '/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int, Title:chararray, Genres:chararray);
RatingFile = LOAD '/Spring-2016-input/ratings.dat' using PigStorage(':') as (UserID:int, MovieID:int, Rating:int, Timestamp:chararray);

groupMovieFile = group MovieFile by MovieID;
groupRatingFile = group RatingFile by MovieID;
joinResult = join groupMovieFile by group, groupRatingFile by group;
formatAsCogroup = foreach joinResult generate groupMovieFile::group as groupID,
                                                groupRatingFile::RatingFile as RatingFile,
                                                groupMovieFile::MovieFile as MovieFile;
limitResult = limit formatAsCogroup 5;
dump limitResult



--############# Ques 3 ##################
register /home/010/z/zx/zxx140430/PIG_UDF/pig_udf.jar;
MovieFile = LOAD '/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int, Title:chararray, Genres:chararray);
formatResult = foreach MovieFile generate Title, FORMAT_GENRE(Genres);
