*Importing the raw datafile;
data Movie(drop=_:);
  infile "/home/u47525655/Assignment/movies.dat" dlmstr='::'   encoding='wlatin1';
  Input MovieID Title :$100. _Genre_Str :$50.;
length Year 4.;
	Year=substr(Title, length(Title)-4, 4);
  array Genre {5} $20.;

  do _i=1 to dim(Genre);
    Genre[_i]=scan(_Genre_str,_i,'|'); 
    if missing(Genre[_i]) then leave;
  end;  
  
data Ratings ;
	Infile "/home/u47525655/Assignment/ratings.dat" dlmstr='::' ;
Input UserID MovieID Ratings Timestamp;
Timestamp=Timestamp+"01jan1970 0:0:0"dt;
Format Timestamp DATETIME19.;
	Rating_Month=month(datepart(Timestamp));
	Rating_Year=year(datepart(Timestamp));
run;

data Users ;
	Infile "/home/u47525655/Assignment/users.dat" dlmstr='::' ;
	Input UserID Gender $ Age $:9. Occupation $ :21. Zipcode $ :10.;
run;


*Making the user dataset readable; 
data Cleaned_Users;
	Set Users;
	if Age eq 1 then age="Under 18";
		else if Age eq 18 then Age="18-24";
		else if Age eq 25 then Age="25-34";
		else if Age eq 35 then age="35-44";
		else if Age eq 45 then Age="45-49";
		else if Age eq 50 then Age="50-55";
		else if Age eq 56 then Age="56+";
	
	if Occupation eq 0 then Occupation="other"; 
		else if Occupation eq 1 then Occupation="academic/educator";
		else if Occupation eq 2 then Occupation="artist";
		else if Occupation eq 3 then Occupation="clerical/admin";
		else if Occupation eq 4 then Occupation="college/grad student";
		else if Occupation eq 5 then Occupation="customer service";
		else if Occupation eq 6 then Occupation="doctor/health care";
		else if Occupation eq 7 then Occupation="executive/managerial";
		else if Occupation eq 8 then Occupation="farmer";
		else if Occupation eq 9 then Occupation="homemaker";
		else if Occupation eq 10 then Occupation="K-12 student";
		else if Occupation eq 11 then Occupation="lawyer";
		else if Occupation eq 12 then Occupation="programmer";
		else if Occupation eq 13 then Occupation="retired";
		else if Occupation eq 14 then Occupation="sales/marketing";
		else if Occupation eq 15 then Occupation="scientist";
		else if Occupation eq 16 then Occupation="self-employed";
		else if Occupation eq 17 then Occupation="technician/engineer";
		else if Occupation eq 18 then Occupation="tradesman/craftsman";
		else if Occupation eq 19 then Occupation="unemployed";
		else if Occupation eq 20 then Occupation="writer";
	;
run;

*Data exploration;
proc contents data=Movie;
run;

proc contents data=Ratings;
run;

proc contents data=Cleaned_users;
run;







*Objective 1;

*Calculating the average rating for each movie;
Proc summary data=Ratings nway;
    class MovieID;
    var  Ratings ;
    output out = Avg_Rating_Per_Movie mean=Ratings;
run;

*Renaming the _FREQ_ to frequency; 
data Avg_Rating_Per_Movie;
	SET Avg_Rating_Per_Movie;
	DROP _TYPE_;
	RENAME _FREQ_= Frequency;
run;

*To determine the cutoff value;
proc means data=Avg_Rating_Per_Movie mean min q1 median q3 max;
	var Frequency;
run;
	
*Creating a histogram plot;
proc univariate data=Avg_Rating_Per_Movie;
   histogram Frequency;
run;

*Creating a histogram plot for Frequency and Ratings;
proc univariate data=avg_rating_per_movie;
  histogram Frequency;
run;

proc univariate data=avg_rating_per_movie;
	histogram Ratings/vscale=count barlabel=count midpoints=(1 to 5 by 0.5);
run;

*Creating a dataset containing movies with 350 or more reviews;
proc sort data=Avg_Rating_Per_Movie out=Rating_Sorted_Movie ;
by descending ratings;
where Frequency ge 350;
run;


*Left join to match user ratings from user;
proc sql;
	create table Gender_Rated as select * from Ratings left join Cleaned_Users on 
		Ratings.UserID=Cleaned_Users.UserID;
quit;

*To create Number of Male and Female rated per movie for each genre;
Proc summary data=Gender_Rated nway;
	class MovieID Gender;
	var Ratings;
	output out=want mean=Average_User_Ratings;
run;

proc sql;
	create table Gender_Rating_Score as select * from Rating_Sorted_Movie left join 
		want on Rating_Sorted_Movie.MovieID=want.MovieID;
quit;

*To read number of males, females into combined way and drop unnecssary variables from the dataset;
data Gender;
	length cat $200;
	length cat1 $200;
	retain cat cat1;
	set Gender_Rating_Score;
	by MovieID notsorted;

	if first.MovieID then
		cat=cats(_FREQ_);
	else
		cat=catx(',', cat, _FREQ_);

	if last.MovieID then
		output;
	drop _FREQ_;

	if first.MovieID then
		cat1=cats(Gender);
	else
		cat1=catx(',', cat1, Gender);

	if last.MovieID then
		output;
	drop Gender;
	drop _TYPE_;
	drop Average_User_Ratings;
run;

*To remove duplication of number of males and females values;
PROC SORT DATA=Gender OUT=Movies_Sorted_Without_DupKey NODUPKEYS;
	BY MovieID cat1;
	where cat1='F,M';
RUN;

PROC SORT DATA=Gender OUT=Movies_Sorted_Without_DupKey1 NODUPKEYS;
	BY MovieID cat1;
	where cat1='M,F';
RUN;

*To split delimiter of number of males and females into array;
data want2;
	set Movies_Sorted_Without_Dupkey1;
	length var1-var2 $10.;
	array var(2) $;

	do i=1 to dim(var);
		var[i]=scan(cat, i, ',', 'M');
	end;
	rename var1=Male;
	rename var2=Female;
	drop cat cat1;
run;

data want1;
	set Movies_Sorted_Without_Dupkey;
	length var1-var2 $10.;
	array var(2) $;

	do i=1 to dim(var);
		var[i]=scan(cat, i, ',', 'M');
	end;
	rename var1=Female;
	rename var2=Male;
	drop cat cat1;
run;

*To merge datasets;
data Rating_Sorted_Movie1;
	set want2 want1;
run;

*To make frequency of users vote per movie in order;
proc sort data=Rating_Sorted_Movie1;
by descending Frequency;
run;


* left join the tables to get movie titles;
proc sql;
	create table Top_Movies as
	select Title, Frequency,Male,Female, Ratings,Year,Genre1,Genre2,Genre3,Genre4,Genre5 from Rating_Sorted_Movie1
	left join Movie on Rating_Sorted_Movie1.MovieID = Movie.MovieID
	order by Rating_Sorted_Movie1.ratings desc;
quit;





*The top 5 movies in each genre;
title "Top 5 Movies' Average Ratings Score From MovieLens";
proc print data=Top_Movies (keep=Title Frequency Ratings Year obs=5);
run;
title;	



*Top 5 Action movies;
proc sql;
	create table top5Action as
	select * from Top_Movies (obs=5)
	where Genre1 = "Action" OR Genre2 = "Action" OR Genre3 = "Action" OR Genre4 = "Action" OR Genre5 = "Action";
quit;

title "Top 5 Action Movies";
proc print data=top5action;
run;
title;
	
*Top 5 Adventure movies;
proc sql;
	create table top5Adventure as
	select * from Top_Movies (obs=5)
	where Genre1 = "Adventure" OR Genre2 = "Adventure" OR Genre3 = "Adventure" OR Genre4 = "Adventure" OR Genre5 = "Adventure";
quit;

title "Top 5 Adventure Movies";	
proc print data=top5Adventure;
run;
title;
	
*Top 5 Animation movies;
proc sql;
	create table top5Animation as
	select * from Top_Movies (obs=5)
	where Genre1 = "Animation" OR Genre2 = "Animation" OR Genre3 = "Animation" OR Genre4 = "Animation" OR Genre5 = "Animation";
quit;
	
title "Top 5 Animation Movies";
proc print data=top5Animation;
run;
title;

*Top 5 Children's movies;
proc sql;
	create table top5Childrens as
	select * from Top_Movies (obs=5)
	where Genre1 = "Children's" OR Genre2 = "Children's" OR Genre3 = "Children's" OR Genre4 = "Children's" OR Genre5 = "Children's";
quit;

title "Top 5 Children's Movies";
proc print data=top5Childrens;
run;
title;
	
*Top 5 Comedy movies;
proc sql;
	create table top5Comedy as
	select * from Top_Movies (obs=5)
	where Genre1 = "Comedy" OR Genre2 = "Comedy" OR Genre3 = "Comedy" OR Genre4 = "Comedy" OR Genre5 = "Comedy";
quit;

title "Top 5 Comedy Movies";
proc print data=top5Comedy;
run;
title;
	
*Top 5 Crime movies;
proc sql;
	create table top5Crime as
	select * from Top_Movies (obs=5)
	where Genre1 = "Crime" OR Genre2 = "Crime" OR Genre3 = "Crime" OR Genre4 = "Crime" OR Genre5 = "Crime";
quit;

title "Top 5 Crime Movies";
proc print data=top5Crime;
run;
title;
	
*Top 5 Documentary movies;
proc sql;
	create table top5Documentary as
	select * from Top_Movies (obs=5)
	where Genre1 = "Documentary" OR Genre2 = "Documentary" OR Genre3 = "Documentary" OR Genre4 = "Documentary" OR Genre5 = "Documentary";
quit;

title "Top 5 Documentary Movies";
proc print data=top5Documentary;
run;
title;
	
*Top 5 Drama movies;
proc sql;
	create table top5Drama as
	select * from Top_Movies (obs=5)
	where Genre1 = "Drama" OR Genre2 = "Drama" OR Genre3 = "Drama" OR Genre4 = "Drama" OR Genre5 = "Drama";
quit;

title "Top 5 Drama Movies";
proc print data=top5Drama;
run;
title;
	
*Top 5 Fantasy movies;
proc sql;
	create table top5Fantasy as
	select * from Top_Movies (obs=5)
	where Genre1 = "Fantasy" OR Genre2 = "Fantasy" OR Genre3 = "Fantasy" OR Genre4 = "Fantasy" OR Genre5 = "Fantasy";
quit;

title "Top 5 Fantasy Movies";
proc print data=top5Fantasy;
run;
title;
	
*Top 5 Film-Noir movies;
proc sql;
	create table top5FilmNoir as
	select * from Top_Movies (obs=5)
	where Genre1 = "Film-Noir" OR Genre2 = "Film-Noir" OR Genre3 = "Film-Noir" OR Genre4 = "Film-Noir" OR Genre5 = "Film-Noir";
quit;

title "Top 5 Film-Noir Movies";
proc print data=top5FilmNoir;
run;
title;
	
*Top 5 Horror movies;
proc sql;
	create table top5Horror as
	select * from Top_Movies (obs=5)
	where Genre1 = "Horror" OR Genre2 = "Horror" OR Genre3 = "Horror" OR Genre4 = "Horror" OR Genre5 = "Horror";
quit;

title "Top 5 Horror Movies";
proc print data=top5Horror;
run;
title;
	
*Top 5 Musical movies;
proc sql;
	create table top5Musical as
	select * from Top_Movies (obs=5)
	where Genre1 = "Musical" OR Genre2 = "Musical" OR Genre3 = "Musical" OR Genre4 = "Musical" OR Genre5 = "Musical";
quit;

title "Top 5 Musical Movies";
proc print data=top5Musical;
run;
title;
	
*Top 5 Mystery movies;
proc sql;
	create table top5Mystery as
	select * from Top_Movies (obs=5)
	where Genre1 = "Mystery" OR Genre2 = "Mystery" OR Genre3 = "Mystery" OR Genre4 = "Mystery" OR Genre5 = "Mystery";
quit;

title "Top 5 Mystery Movies";
proc print data=top5Mystery;
run;
title;
	
*Top 5 Romance movies;
proc sql;
	create table top5Romance as
	select * from Top_Movies (obs=5)
	where Genre1 = "Romance" OR Genre2 = "Romance" OR Genre3 = "Romance" OR Genre4 = "Romance" OR Genre5 = "Romance";
quit;

title "Top 5 Romance Movies";
proc print data=top5Romance;
run;
title;
		
*Top 5 Sci-Fi movies;
proc sql;
	create table top5SciFi as
	select * from Top_Movies (obs=5)
	where Genre1 = "Sci-Fi" OR Genre2 = "Sci-Fi" OR Genre3 = "Sci-Fi" OR Genre4 = "Sci-Fi" OR Genre5 = "Sci-Fi";
quit;

title "Top 5 Sci-Fi Movies";
proc print data=top5SciFi;
run;
title;
	
*Top 5 Thriller movies;
proc sql;
	create table top5Thriller as
	select * from Top_Movies (obs=5)
	where Genre1 = "Thriller" OR Genre2 = "Thriller" OR Genre3 = "Thriller" OR Genre4 = "Thriller" OR Genre5 = "Thriller";
quit;

title "Top 5 Thriller Movies";
proc print data=top5Thriller;
run;
title;

*Top 5 War movies;
proc sql;
	create table top5War as
	select * from Top_Movies (obs=5)
	where Genre1 = "War" OR Genre2 = "War" OR Genre3 = "War" OR Genre4 = "War" OR Genre5 = "War";
quit;

title "Top 5 War Movies";
proc print data=top5War;
run;
title;
	
*Top 5 Western movies;
proc sql;
	create table top5Western as
	select * from Top_Movies (obs=5)
	where Genre1 = "Western" OR Genre2 = "Western" OR Genre3 = "Western" OR Genre4 = "Western" OR Genre5 = "Western";
quit;

title "Top 5 Western Movies";
proc print data=top5Western;
run;
title;

*Calculate total number of Male and Female who rated top 5 movies of each genre;
data Total_Gender;
set Top_Movies;
Total_Male+Male;
Total_Female+Female;

proc sql;
	create table Total_Gender_List as select *, monotonic() as cnt from Total_Gender having 
		cnt between max(cnt)-0 and max(cnt);
quit;


Data genderfrequency(keep=Total_Male total_Female);
	set Total_Gender_List;
run;


proc transpose data=genderfrequency out=Transformed_Gender_List(rename=(_name_=Gender COL1=Frequency));
var Total_Male Total_Female;
run;

*Creating a barchart;
title 'Total Number of Genders Who Rated in Each Top 5 movies of Each Genre';
proc SGPLOT data = Transformed_Gender_List;
vbarparm category=Gender response= Frequency;
xaxis display=(nolabel noticks);
run;
title;








	
*Objective 2;

*To calculate the numbers of movie produced in a year and to find the year that has the highest number of movie produced;
proc freq data=Movie;
	table Year/ out = Movie_Per_Year;
run;

*Scatter plot graph;
TITLE 'Number of movie produced in each year';
proc sgscatter data=Movie_Per_Year;
plot Count*Year
/ datalabel = Year  grid;
run;
title;



*Create a table with only movies that is created in the year 1996;
proc sql; 
	create table Popular_Movies_Year  as
	select *
	from Movie
	where Year=1996;
quit;
	
	
*Creating a counter to count the number of each genre for each movie in the year 1996;	
data Count_Genre (drop=_: keep = action adventure animation children comedy crime documentary
	drama fantasy filmnoir horror musical mystery
	romance scifi thriller war western);
	set Popular_Movies_Year;
	array Genre{5} $20.;
	
	do _i=1 to dim(genre);
		if genre[_i] eq 'Action' then Action + 1;
		if genre[_i] eq 'Adventure' then Adventure + 1;
		if genre[_i] eq  'Animation' then Animation+1;
		if genre[_i] eq  "Children's" then Children+1;
		if genre[_i] eq  'Comedy' then Comedy+1;
		if genre[_i] eq  'Crime' then Crime+1;
		if genre[_i] eq  'Documentary' then Documentary+1;
		if genre[_i] eq  'Drama' then Drama+1;
		if genre[_i] eq  'Fantasy' then Fantasy+1;
		if genre[_i] eq  'Film-Noir' then FilmNoir+1;
		if genre[_i] eq  'Horror' then Horror+1;
		if genre[_i] eq  'Musical' then Musical+1;
		if genre[_i] eq  'Mystery' then Mystery+1;
		if genre[_i] eq  'Romance' then Romance+1;
		if genre[_i] eq  'Sci-Fi' then Scifi+1;
		if genre[_i] eq  'Thriller' then Thriller+1;
		if genre[_i] eq  'War' then War+1;
		if genre[_i] eq  'Western' then Western+1;
		else;
	end;	
run;

*Creating a table that summarize how many movie is produced for each genre;
proc sql;
	create table Genre_List as select *,monotonic() as cnt from Count_Genre
	having cnt between max(cnt)-0 and max(cnt);
	quit;

*To transpose the column into row;
proc transpose data=Genre_List out=Transformed_Genre_List(rename=(_name_=Genre COL1=Frequency));
var Action Adventure Animation Children Comedy Crime Documentary Drama Fantasy FilmNoir Horror Musical Mystery Romance SciFi Thriller War Western;
run;

*Creating a barchart;
title 'Genre produced in 1996';
proc SGPLOT data = Transformed_Genre_List;
vbarparm category=Genre response= Frequency;
title 'Genre produced in 1996';
xaxis display=(nolabel noticks);
run;
title;

*To identify which year have the most rating by the users;
proc freq data=Ratings order=freq;
	table Rating_Year;


*As we find out that drama,comedies is the most popular of 1996 movie genres 
we create a plot for users  who watch drama and comedies movies within 2000 to 2003;
proc sql;
	create table Movierated as select * from Popular_Movies_Year left join Ratings 
		on Popular_Movies_Year.MovieID=Ratings.MovieID where Rating_Year=2000 
		and (Genre1='Drama' or Genre1='Comedy' or Genre2='Drama' or Genre2='Comedy' 
		or Genre3='Drama' or Genre3='Comedy' or Genre4='Drama' or Genre4='Comedy' or 
		Genre5='Drama' or Genre5='Comedy');
quit;

PROC UNIVARIATE DATA=Movierated NOPRINT;
	VAR Rating_Month;
	ods graphics on;
	HISTOGRAM /odstitle='Month of Watching 1996 Movies in Year 2000' midpoints=(1 
		to 12 by 1) vscale=count barlabel=count CFRAME=GRAY CAXES=BLACK WAXIS=1 
		CBARLINE=BLACK CFILL=BLUE PFILL=SOLID;
run;

proc sql;
	create table Movierated1 as select * from Popular_Movies_Year left join 
		ratings on Popular_Movies_Year.MovieID=ratings.MovieID where 
		Rating_Year=2001 and (Genre1='Drama' or Genre1='Comedy' or Genre2='Drama' or 
		Genre2='Comedy' or Genre3='Drama' or Genre3='Comedy' or Genre4='Drama' or 
		Genre4='Comedy' or Genre5='Drama' or Genre5='Comedy');
quit;

PROC UNIVARIATE DATA=Movierated1 NOPRINT;
	VAR Rating_Month;
	ods graphics on;
	HISTOGRAM /odstitle='Month of Watching 1996 Movies in Year 2001' midpoints=(1 
		to 12 by 1) vscale=count barlabel=count CFRAME=GRAY CAXES=BLACK WAXIS=1 
		CBARLINE=BLACK CFILL=BLUE PFILL=SOLID;
run;

proc sql;
	create table Movierated2 as select * from Popular_Movies_Year left join 
		ratings on Popular_Movies_Year.MovieID=ratings.MovieID where 
		Rating_Year=2002 and (Genre1='Drama' or Genre1='Comedy' or Genre2='Drama' or 
		Genre2='Comedy' or Genre3='Drama' or Genre3='Comedy' or Genre4='Drama' or 
		Genre4='Comedy' or Genre5='Drama' or Genre5='Comedy');
quit;

PROC UNIVARIATE DATA=Movierated2 NOPRINT;
	VAR Rating_Month;
	ods graphics on;
	HISTOGRAM /odstitle='Month of Watching 1996 Movies in Year 2002' midpoints=(1 
		to 12 by 1) vscale=count barlabel=count CFRAME=GRAY CAXES=BLACK WAXIS=1 
		CBARLINE=BLACK CFILL=BLUE PFILL=SOLID;
run;

proc sql;
	create table Movierated3 as select * from Popular_Movies_Year left join 
		ratings on Popular_Movies_Year.MovieID=ratings.MovieID where 
		Rating_Year=2003 and (Genre1='Drama' or Genre1='Comedy' or Genre2='Drama' or 
		Genre2='Comedy' or Genre3='Drama' or Genre3='Comedy' or Genre4='Drama' or 
		Genre4='Comedy' or Genre5='Drama' or Genre5='Comedy');
quit;

PROC UNIVARIATE DATA=Movierated3 NOPRINT;
	VAR Rating_Month;
	ods graphics on;
	HISTOGRAM /odstitle='Month of Watching 1996 Movies in Year 2003' midpoints=(1 
		to 12 by 1) vscale=count barlabel=count CFRAME=GRAY CAXES=BLACK WAXIS=1 
		CBARLINE=BLACK CFILL=BLUE PFILL=SOLID;
run;




