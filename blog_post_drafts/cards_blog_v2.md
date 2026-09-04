---
layout: post 
title: "Whats in the Cards? How is the increased scrutiny and punishment since 2017 changing Rugby"
description: "This is the description of the first blog post for a possible Rugby Data Analysis blog"
date: 2025-11-23T02:53:00-7:00
tags: Rugby, Stats, Analysis
image:
---

# "Whats in the Cards? Part 1: How is the increased scrutiny and punishment since 2017 changing Rugby"

![Penalties Blog Title Image](../test_title_img_2.jpg)


----

The greatest time of year is once again here. Six Nations Rugby. 

Since adding France in 2000, Six Nations has been one of the longest running annual international tournaments across sports, and largely untouched in format, unlike the game being played on the field. We are now 3 weeks into the 2026 tournament, and in my opinion it has been been one of the most exciting in recent years at this point in the tournament. Ireland and Scotland fell and started rising. England came in hot and fell hard. Italy has had one of its best starts. France is unstoppable. And Wales looks marginally better and will probably have a very exciting game against Italy for wooden spoon. 

15 cards so far in the first 3 rounds. 


Every sport has a duty to prioritize the safety, health, and wellness of their players. Not every sport is the same in those respects, but every league still has the same responsibility. Worldwide and across leagues, this has been tested in the last decade. 

Rugby has seen changes that have been aimed to impact the strategy of play for both teams, but predominately rules changes in the last decade have been focused on player safety. As a rugby player myself, I have certainly seen the changes. When I was playing, a high tackle that was not blatantly malicious was just a penalty, while now a high tackle is most often a yellow card at least. 

My playing career is over now, but I still absorb as much Rugby as possible. Combining my passion for Rugby and my Data Science knowledge, my goal is to attempt to explain the impact of these rule changes using data. 

I have been working on a centralized Rugby database for all levels of professional Rugby, scraping and indexing match and player data from across the web. Analyzing this data, its clear how disciplinary rules changes have impacted the match competitiveness and sport on the whole. 


### Disciplinary Changes (2011 - 2023) 

Before getting to the data, its important to see all the changes to consider. 

In late 2016 and early 2017, World Rugby introduced a littany of new rules. The most prominent change was the increased sanctions and review of high tackles. The intended result of the change was to decrease the number of dangerous tackles by more severly penalizing the tackler. 

Adding onto this change at the 2023 Rugby World Cup, a booth-review upgrade system was introduced allowing officials to review yellow cards during the 10 minutes period and possibly upgrade to a red card. This had immediate impacts on the game, with a yellow card being upgraded to a red for New Zealand within the first 10 minutes of the Championship match against South Africa, a Championship South Africa would go on to win for the second time in a row. 

The 2023 Championship game ended up with a total of 3 yellow cards, and the 1 booth-reviewed upgraded red card. In the previous 9 World Cup finals, there had been only 1 card issued. Clearly the new regulations were showing an impact. 

At the end of 2025, Ireland hosted South Africa in the culmination of the Autumn International Matches for both teams. The 3rd time the two titans had squared off since their pool match in the 2023 World Cup. Even since that match with a win each the previous year, huge anticipation. What we got instead was a match that broke the record for the most cards given out in an international rugby game. Ireland would see 4 yellow cards and 1 red card, and South Africa would see a yellow card for a total of 6 total cards. The red would be an upgraded yellow, and all but 1 of those Irish cards would be before halftime, stealing a large part of the competition for the second half. 

The increase in cards are changing the way the game is played. Using the data I have gathered, my goal was to to see how team competition has changed, how well teams have been able to fight through increased scrutiny, and !!!!!!


![Red Cards Over Time (2010-2024)](../scrape_code/test_image_cards.png)

### UPDATE AXIS AND LEGEND LABELS 

(Lack of data avilable for the 2022-2023 URC season, no everyone did not just behave themselves that season)


## Pro Club Level Review 

Significantly more club level games are played every year compared to international squads, so the impact on the game is more quantifiable. For this analysis, I reviewed the 3 European Pro leagues, the United Rugby Championship, The Top 14, and the English Premiership. 

### Top 14 - French League 

### United Rugby Championship League 

### English Premiership League 


### Are teams playing within the rules?

GRAPH OF TOTAL PENALTIES OVER TIME 

Over time we have not seen a significant change in the number of penalties. An increase in cards coupled with consistent penalties would leave me to assume that players are acting the same in game as before the rule changes, these actions are now just scrutinized harder. Really does make you wonder when we will see these card amounts will start to dip down and react more to the newer rules. 

### Comebacks after getting carded 

A key metric I wanted to analyze was how teams are reacting now after they get carded. Can a team bounce back and stay in the game after being carded? Or does the opposing team run away with the rest of the game?

The way I am evaluating how leagues in this first pass of analysis is by looking at 

GRAPH OF SCORE DIFF AT TIME OF RED CARD 

AVERAGE POINTS SCORED BY CARDED TEAM AFTER FIRST CARD 

The interesting part of this finding to me is the difference across leagues. 

AVERAGE SCORE MARGIN RED CARDS AND TOTAL 

### How do increased Cards impact winning percentage?

When red cards were less common, that loss of a player was certainly a tough task for the remaining 14 men. So with the increase in frequency, teams can adapt in multiple ways. Does a red card still seal the game, or have teams started learning to play better with less players? Could red cards be so frequent that the likelihood of the opposing team getting one after you out-weigh the disadvantage? How does timing of these cards impact the chance of winning?

To attempt to answer some of these questions, I set up a predictive model to control certain variables that might impact in-game winning percentages. The current structure is a Logistic Regression model that will predict the win probability of the home team of a given game, the following variables:

1. Difference in Ranking: Difference in the league standing between the home and away team. (Negative if the away team is higher ranked than the home team)

2. Red Card Season Interaction (Key Interaction term)

2. Number of Home Yellow Cards 

3. Number of Home Red Cards (Centered on the Mean)

4. Number of Away Yellow Cards 

5. Number of Away Red Cards 

6. Home Total Possession % 

7. Home Total Territory Gained 

6. Red Cards in the first half 

7. Red Cards in first 20 minutes of the second half 

8. Red Cards in the last 20 minutes of the game 

9. Season: Binary variable for if the game is being player before 2017 or after the 2017 season (Pre and Post rule changes)

10. League: Dummy variable for which league the game is being played in (URC, T14, or Prem) THis variable will not be used for an international match model 


An important note for this model: The intention is not to predict whether or not a team will win if a team recevies a red card, I care more about quantifiy how more worse a team's chances of winning are if they get a red card, and if there is a difference in that impact pre and post rule change. Another note on league ranking, at the time I only have the end of season data on the ranking so that will have to work as a proxy for the relative position of the teams, I can understand the potential issues with this approach so feel free to yell at me if its grossly out of line. 

While there is a significant possibility of missed multicollinearity in this model, due to multiple match variables being correlated with each other and with winning, this inital framework is believed to be a strong statistical model of a match, as opposed to predicting match outcomes. 

The goal is to see how post rule change red cards are impacting the game, compared to red cards in general from previous years. To quantify this difference, I create an interaction feature between season and red cards. This interaction term is defined in the following way:

$$ 
RC_{INT} = RC * S \\[1em]
S = \begin{cases} 1 & \text{if Season} \geq 2017 \\ 0 & \text{if Season} < 2017 \end{cases} \\[1em]
$$

Including red cards and seasons as seperate features in the model can causes issues with multicollinearity, so to account for that, all relevant features that might suffer are centerted on the feature mean. 

From the fitted model on the league data, the coefficients that we are zoning in on are the RC Interaction term, and the total red cards. The RC Interaction term had a postive value of 0.5546 with a p-value of ~0.191. For the centered red cards feature, we see almost an inverse relationship of the coeficients, with -0.3528 and a stronger p-value of 0.086. Now, of course red cards for a team in any situation should negativly impact the winning percentage for a team, and the RC interaction coefficient is not saying the contrary. Instead, because total red cards was included in the model as well, it can be interpreted so that red cards incurred post 2017 rule change are more positive on the winning percentage than cards incurred before that change. 

The coefficients therefore offer some evidence into how teams have been adapting to the changes. Teams appear to not be as hampered by red cards in the new set of rules, most likely in my opinon due to the likelihood of the opposing team incurring one as well 

Using this coefficien.We don't see significance at the 5% level for this coefficient, 

Using this win probability estimation, lets look at some of the games with red cards and how they differ. 

GRAPHS OF GAMES WITH WIN PROBABILITY AND NUMBER OF RED CARDS 



### Conclusion and next steps



