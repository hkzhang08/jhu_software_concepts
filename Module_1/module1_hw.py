"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_1 - Personal Website
Due Date:      January 25th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""



# Skills: Flask, HTML, Searching Methods, Data Cleaning, JSON Data Object Storage


# Import html file for home page (include name, position, bio, picture)
from flask import Flask
from Module_1.pages import pages

app = Flask(__name__)
app.register_blueprint(pages)
