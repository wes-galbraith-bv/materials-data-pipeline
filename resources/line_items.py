from flask import Blueprint

from models import LineItem

line_items = Blueprint('line_items', '__name__')

@line_items.route('/line-items/hi')
def say_hi():
    return '<h1>hello</h1>'

@line_items.route('/line-items/item-no/<item_no>/project-site-id/<project_site_id>/po-no/<po_no>')
def get_items_by_site_id(item_no, project_site_id, po_no):
    data = LineItem.query(item_no, project_site_id, po_no)
