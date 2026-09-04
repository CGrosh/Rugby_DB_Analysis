---
layout: post 
title: "Whats in the Cards? How is the increased scrutiny and punishment since 2017 changing Rugby"
description: "This is the description of the first blog post for a possible Rugby Data Analysis blog"
date: 2025-11-23T02:53:00-7:00
tags: Rugby, Stats, Analysis
image:
---

# "Whats in the Cards? How is the increased scrutiny and punishment since 2017 changing Rugby"

![Penalties Blog Title Image](../test_title_img_2.jpg)


----

----

Every sport has a duty to prioritize the safety, health, and wellness of their players. Not every sport is the same in those respects but, every league still has the same responsibility. Worldwide and across leagues, this has been tested in the last decade. 

In the NBA, star players Damian Lillard and Jayson Tatum both had their postseason end with torn Achilles Tendons by the second round. More notably, young star Tyrese Haliburton exited the clinching game of the finals within the first half with the same injury. A game, and title, his team would go on to lose to the OKC Thunder.  

In baseball, knee and head injuries are of course far less common, while the arm instead carries the burden. The value of a baseball player often resides in their arm. One major competitive advantage for a pitching prospect is the Tommy-John Procedure, with the assumption being eventually they are going to throw their arm out and need it anyways. For my non-US readers, Tommy-John Surgery is a procedure where ligaments in the elbow are reconstructed from harvested ligaments, often from the forearm or leg, 

## How are these new rules impacting the game? 

### Yellow and Red Cards over time (2011 - 2023) 

In late 2016 and early 2017, World Rugby introduced a littany of new rules. The most prominent change was the increased sanctions and review of high tackles. The intended result of the change was to decrease the number of dangerous tackles by more severly penalizing the tackler. 

As a rugby player myself, I have certainly seen the changes. When I was playing, a high tackle that was not blatantly malicious was just a penalty, while now a high tackle is most often a yellow card at least. Adding onto this change at the 2023 Rugby World Cup, a booth-review upgrade system was introduced allowing officials to review yellow cards during the 10 minutes period and possibly upgrade to a red card. This had immediate impacts on the game, with a yellow card being upgraded to a red for New Zealand within the first 10 minutes of the Championship match against South Africa, a Championship South Africa would go on to win for the second time in a row. 

The 2023 Championship game ended up with a total of 3 yellow cards, and the 1 booth-reviewed upgraded red card. In the previous 9 World Cup finals, there had been only 1 card issued. Clearly the new regulations were showing an impact. 

![Red Cards Over Time (2010-2024)](../scrape_code/test_image_cards.png)
(Lack of data avilable for the 2022-2023 season, no everyone did not just behave themselves that season)

As expected have seen dramatic increases in 


![Yellow Cards Over Time (2010-2024)](../scrape_code/test_image_cards_yellow.png)



## Are they being applied evenly? (Home vs Away, League, Specific Team )

I'm not just curious about if there are more red cards being given out since the rule changes in 2017, obviously there are. What is more interesting to me is what is where are we seeing them issued more often. There are multiple leagues in Europe alone, which league is struggling to adjust and racking up the cards? What teams have the largest change in issued cards post rule change? Are the best teams avoiding cards? Lets see from the data 

HOME RC PER GAME VS AWAY RC PER GAME Graph 

As expected, the amount of red cards oer game 


HOME YC PER GAME VS AWAY YC PER GAME Graph 


MOST CARDED TEAMS OVERALL, LEAST CARDED TEAMS, BEFORE AND AFTER RULE CHANGE FOR THOSE TEAMS 

## With the increased amount of Cards, is it swaying the outcome as much as it used to? 

If teams are seeing a card appear more often 

## Are Increased Cards making matches more or less competitive?



## Marginal Win Probability loss per card 

## Best Teams that can play through cards 






That’s a great angle for a rugby analytics blog—cards are one of the clearest rule- and referee-driven interventions in the modern game. Below are **interesting, data-driven questions** you can ask, grouped by theme, with notes on *why they matter* and *how you might answer them quantitatively*.

---

## 1. Match Outcomes & Competitive Balance

### 1. Does receiving a yellow/red card materially reduce a team’s probability of winning?

**Why it’s interesting:** Common belief, but the magnitude matters.
**How to analyze:**

* Logistic regression: `win ~ cards_for + cards_against + home + ranking_diff`
* Separate yellow vs red
* Estimate marginal win probability loss per card

---

### 2. Has the *impact* of a card on winning increased since 2017?

**Why:** Cards may be more frequent, but teams might also be better at adapting.
**Method:**

* Interaction term: `card * post_2017`
* Compare win-rate deltas pre- vs post-2017

---

### 3. Are matches with cards more or less competitive?

**Metrics:**

* Final score margin
* Lead changes
* Time spent within one score

**Hypothesis:** More cards → larger margins, fewer competitive finishes.

---

## 2. Timing & Game State Effects

### 4. When is a card most damaging?

**Examples:**

* First 20 minutes vs last 20 minutes
* While leading vs trailing

**Method:**

* Segment cards by time bins
* Compare point differential *after* card issuance

---

### 5. Do teams score more efficiently during card windows post-2017?

**Why:** Tactical sophistication may have improved.
**Metrics:**

* Points per possession
* Tries per 10 minutes during card periods
* Expected points added (EPA)

---

### 6. How often do teams *survive* a yellow card without conceding?

**Method:**

* Binary outcome during sin-bin window: concede points (Y/N)
* Trend over time since 2017

---

## 3. Discipline & Playing Style

### 7. Are certain teams or leagues driving the increase in cards?

**Why:** Could be cultural or tactical.
**Analysis:**

* Cards per match by team / competition
* Random effects model (team-level variance)

---

### 8. Has the distribution of card types changed?

**Examples:**

* Dangerous tackles vs breakdown infringements
* Professional fouls vs technical penalties

**If data allows:**

* NLP or categorical grouping of referee descriptions

---

### 9. Are “high-tempo” teams penalized more?

**Proxies for tempo:**

* Rucks per minute
* Passes per phase
* Ball-in-play time

**Test:**

* Correlation or regression with card counts

---

## 4. Refereeing & Law Interpretation

### 10. Did card rates jump immediately after 2017 law changes—or gradually?

**Why:** Helps distinguish policy shock vs cultural change.
**Method:**

* Structural break analysis
* Interrupted time series

---

### 11. Are referees more consistent now than pre-2017?

**Metrics:**

* Variance in cards per match by referee
* Gini coefficient or coefficient of variation

---

### 12. Do TMO interventions correlate with card inflation?

**Method:**

* % of cards initiated by TMO
* Trend analysis over time

---

## 5. Player-Level Impact

### 13. Are certain positions disproportionately carded post-2017?

**Hypothesis:**

* Forwards (esp. back-row) affected more by breakdown policing

**Method:**

* Cards per 1000 minutes by position
* Pre/post comparison

---

### 14. Do repeat offenders exist, or are cards broadly distributed?

**Why:** Discipline problem vs systemic enforcement.
**Metrics:**

* Herfindahl index for card concentration
* Survival analysis: time to next card

---

## 6. Tactical Adaptation

### 15. Have teams changed substitution patterns due to cards?

**Ideas:**

* Earlier front-row subs
* Replacing back-row after yellow

**Method:**

* Sub timing analysis conditional on card events

---

### 16. Are teams kicking more while down a player?

**Metrics:**

* Kick-to-pass ratio during card windows
* Territory gain per minute

---

## 7. Fan Experience & Game Flow

### 17. Are games with more cards actually lower quality?

**Proxies for “quality”:**

* Handling errors
* Stoppages per minute
* Ball-in-play time

---

### 18. Has the average match narrative changed?

**Examples:**

* More “momentum swings”
* Higher variance in scoring

**Method:**

* Rolling win probability charts
* Entropy of scoring sequences

---

## 8. Predictive & Counterfactual Questions (Great for Advanced Posts)

### 19. What would match outcomes look like under 10-minute vs 20-minute reds?

**Method:**

* Counterfactual simulation using scoring rates during card windows

---

### 20. Can we predict a card before it happens?

**Features:**

* Recent penalties conceded
* Defensive workload
* Field position pressure

**Model:**

* Hazard model or rolling probability model

---

## Suggested Blog Structure (Pro Tip)

For each post:

1. **Question**
2. **Why fans argue about it**
3. **Data & method**
4. **Result (with uncertainty)**
5. **What this means for rugby**

If you want, I can:

* Help you **prioritize questions based on your available data**
* Design a **data schema** for cards + events
* Draft a **first blog post outline** with charts and models

Just tell me what competitions and granularity (match, event, player) you’re working with.
