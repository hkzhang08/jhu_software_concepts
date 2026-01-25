"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_1 - Personal Website
Due Date:      January 25th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""

# Import blueprint and render_template for my html pages
from flask import Blueprint, render_template

# Create blueprint pages
pages = Blueprint("pages", __name__)

# Routing page for homepage
@pages.route("/")
def homepage():
    return render_template('homepage.html')

# Routing page for contact
@pages.route('/contact')
def contact():
    return render_template('contact.html')

# Routing page for projects
@pages.route('/projects')
def projects():
    return render_template('projects.html')
