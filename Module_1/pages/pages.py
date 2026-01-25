

# Import html file for home page (include name, position, bio, picture)
from flask import Flask, render_template

pages = Blueprint("pages", __name__)

@pages.route("/")
def homepage():
    return render_template('homepage.html')

@pages.route('/contact')
def contact():
    return render_template('contact.html')

@pages.route('/projects')
def projects():
    return render_template('projects.html')