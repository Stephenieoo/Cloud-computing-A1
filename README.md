# Cloud-computing-A1
Cloud and Big Data Fall 2025-Assignment 1


🧠 System Overview

This project implements an end-to-end dining recommendation system using AWS services — integrating Lex, SQS, Lambda, OpenSearch, DynamoDB, SES, and EventBridge.

🧩 Workflow
	1.	User Interaction
	    •	The user chats with Lex via the web front end.
	    •	Lex collects user inputs including:
            •	Location
            •	Cuisine type
            •   Dining time
            •	Number of people
            •	Email address
	2.	Message Queue
        •	Lex sends the collected information to an SQS Queue.
	3.	Automated Lambda Trigger
        •	EventBridge triggers Lambda Function LF2 every minute.
	4.	Lambda (LF2) Execution Flow
        •	Reads user requests from the SQS queue.
        •	Randomly retrieves several restaurant IDs from OpenSearch based on the cuisine.
        •	Fetches restaurant details from DynamoDB.
        •	Formats the recommendation results.
        •	Sends an email to the user via Amazon SES.