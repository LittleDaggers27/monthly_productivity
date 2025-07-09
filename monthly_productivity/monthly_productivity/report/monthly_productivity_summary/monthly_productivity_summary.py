import frappe

def execute(filters=None):
    return frappe.db.sql("""WITH detail_rows AS (
  SELECT
    ed.parent AS "Doc:Link/Monthly Productivity",
    ed.sales_invoice AS "Invoice:Link/Sales Invoice",
    si.customer_name AS "Customer:Data",
    mp.report_month AS "Month:Date",
    ed.execution_percentage AS "Execution:Percent",
    si.grand_total AS "Invoice Total:Currency",
    si.grand_total * ed.execution_percentage / 100 AS "Executed Value:Currency",
    COALESCE((
      SELECT SUM(prev.execution_percentage)
      FROM `tabExecution Schedule Entry` prev
      JOIN `tabMonthly Productivity` prev_mp ON prev.parent = prev_mp.name
      WHERE prev.sales_invoice = ed.sales_invoice
        AND prev_mp.docstatus = 1
        AND prev_mp.report_month <= mp.report_month
    ), 0) AS "Cumulative:Percent",
    NULL AS "Total Purchases:Currency",
    NULL AS "Other Expenses:Currency",
    NULL AS "Total Expenses:Currency",
    NULL AS "Profit or Loss:Currency",
    NULL AS "Commission:Currency"
  FROM `tabExecution Schedule Entry` ed
  JOIN `tabMonthly Productivity` mp ON ed.parent = mp.name AND mp.docstatus = 1
  LEFT JOIN `tabSales Invoice` si ON si.name = ed.sales_invoice AND si.docstatus = 1
  WHERE mp.report_month BETWEEN %(from_date)s AND %(to_date)s
),
summary_row AS (
  SELECT
    NULL AS "Doc:Link/Monthly Productivity",
    NULL AS "Invoice:Link/Sales Invoice",
    'TOTAL' AS "Customer:Data",
    NULL AS "Month:Date",
    NULL AS "Execution:Percent",

    COALESCE((
      SELECT SUM(si.grand_total)
      FROM `tabExecution Schedule Entry` ed
      JOIN `tabMonthly Productivity` mp ON ed.parent = mp.name AND mp.docstatus = 1
      LEFT JOIN `tabSales Invoice` si ON si.name = ed.sales_invoice AND si.docstatus = 1
      WHERE mp.report_month BETWEEN %(from_date)s AND %(to_date)s
    ), 0) AS "Invoice Total:Currency",

    COALESCE((
      SELECT SUM(si.grand_total * ed.execution_percentage / 100)
      FROM `tabExecution Schedule Entry` ed
      JOIN `tabMonthly Productivity` mp ON ed.parent = mp.name AND mp.docstatus = 1
      LEFT JOIN `tabSales Invoice` si ON si.name = ed.sales_invoice AND si.docstatus = 1
      WHERE mp.report_month BETWEEN %(from_date)s AND %(to_date)s
    ), 0) AS "Executed Value:Currency",

    NULL AS "Cumulative:Percent",

    -- Total Purchases
    COALESCE((
      SELECT SUM(pi.grand_total)
      FROM `tabPurchase Invoice` pi
      WHERE pi.docstatus = 1 AND pi.posting_date BETWEEN %(from_date)s AND %(to_date)s
    ), 0) AS "Total Purchases:Currency",

    -- Other Expenses (62–69)
    COALESCE((
      SELECT SUM(jea.debit)
      FROM `tabJournal Entry Account` jea
      JOIN `tabJournal Entry` je ON je.name = jea.parent
      WHERE je.docstatus = 1
        AND LEFT(jea.account, 2) BETWEEN '62' AND '69'
        AND je.posting_date BETWEEN %(from_date)s AND %(to_date)s
    ), 0) AS "Other Expenses:Currency",

    -- Total Expenses
    (
      COALESCE((
        SELECT SUM(pi.grand_total)
        FROM `tabPurchase Invoice` pi
        WHERE pi.docstatus = 1 AND pi.posting_date BETWEEN %(from_date)s AND %(to_date)s
      ), 0)
      +
      COALESCE((
        SELECT SUM(jea.debit)
        FROM `tabJournal Entry Account` jea
        JOIN `tabJournal Entry` je ON je.name = jea.parent
        WHERE je.docstatus = 1
          AND LEFT(jea.account, 2) BETWEEN '62' AND '69'
          AND je.posting_date BETWEEN %(from_date)s AND %(to_date)s
      ), 0)
    ) AS "Total Expenses:Currency",

    -- Profit or Loss
    (
      COALESCE((
        SELECT SUM(si.grand_total * ed.execution_percentage / 100)
        FROM `tabExecution Schedule Entry` ed
        JOIN `tabMonthly Productivity` mp ON ed.parent = mp.name AND mp.docstatus = 1
        LEFT JOIN `tabSales Invoice` si ON si.name = ed.sales_invoice AND si.docstatus = 1
        WHERE mp.report_month BETWEEN %(from_date)s AND %(to_date)s
      ), 0)
      -
      (
        COALESCE((
          SELECT SUM(pi.grand_total)
          FROM `tabPurchase Invoice` pi
          WHERE pi.docstatus = 1 AND pi.posting_date BETWEEN %(from_date)s AND %(to_date)s
        ), 0)
        +
        COALESCE((
          SELECT SUM(jea.debit)
          FROM `tabJournal Entry Account` jea
          JOIN `tabJournal Entry` je ON je.name = jea.parent
          WHERE je.docstatus = 1
            AND LEFT(jea.account, 2) BETWEEN '62' AND '69'
            AND je.posting_date BETWEEN %(from_date)s AND %(to_date)s
        ), 0)
      )
    ) AS "Profit or Loss:Currency",

    -- Commission
    COALESCE((
      SELECT SUM(
        (
          (si.grand_total * ed.execution_percentage / 100)
          -
          (
            COALESCE((
              SELECT SUM(pi.grand_total)
              FROM `tabPurchase Invoice` pi
              WHERE pi.docstatus = 1 AND pi.posting_date BETWEEN %(from_date)s AND %(to_date)s
            ), 0)
            +
            COALESCE((
              SELECT SUM(jea.debit)
              FROM `tabJournal Entry Account` jea
              JOIN `tabJournal Entry` je ON je.name = jea.parent
              WHERE je.docstatus = 1
                AND LEFT(jea.account, 2) BETWEEN '62' AND '69'
                AND je.posting_date BETWEEN %(from_date)s AND %(to_date)s
            ), 0)
          )
        ) * (mp.commission_percentage / 100)
      )
      FROM `tabExecution Schedule Entry` ed
      JOIN `tabMonthly Productivity` mp ON ed.parent = mp.name AND mp.docstatus = 1
      LEFT JOIN `tabSales Invoice` si ON si.name = ed.sales_invoice AND si.docstatus = 1
      WHERE mp.report_month BETWEEN %(from_date)s AND %(to_date)s
    ), 0) AS "Commission:Currency"
)

SELECT * FROM detail_rows
UNION ALL
SELECT * FROM summary_row
ORDER BY "Invoice", "Month";
""", filters, as_dict=True)
