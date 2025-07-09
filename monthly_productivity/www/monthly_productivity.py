import frappe
from frappe import _

def get_context(context):
    context.title = _("Monthly Productivity")
    return context
