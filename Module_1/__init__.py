"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_1 - Personal Website
Due Date:      January 25th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""



# Import flask and blueprint for my website
from flask import Flask
from pages import pages

# Setup app
app = Flask(__name__)

# Register the blueprint with flask
app.register_blueprint(pages)
