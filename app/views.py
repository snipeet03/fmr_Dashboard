    from django.shortcuts import render
from django.http import HttpResponse
import pandas as pd
from datetime import datetime, timedelta
import io
from sqlalchemy import create_engine
import urllib

def startup_view(request):
    context = {}

    if request.method == 'POST':
        start_date = request.POST.get('start_date')
        end_date = request.POST.get('end_date')

        # Validate dates
        try:
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            context['error'] = "❌ Incorrect date format. Please use YYYY-MM-DD."
            return render(request, 'fmr_project/home.html', context)

        end_date_dt_exclusive = end_date_dt + timedelta(days=1)

        # DB connection
        server = 'NA0VSQL05'
        database = 'B105_FMR_SQL_DB'
        username = 'BGSW_Admin'
        password = 'BGSW_Admin123'
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        # SQL
        query = f"""
        SELECT 
            zfrd.name, zfrd.actual, zfrd.nominal, zfrd.usl, zfrd.lsl,
            org.OrganizationName, org.OrgnizationDescription, 
            zfrh.drawing_no, ind.CreatedDateTime
        FROM 
            FmrTables.tblZeissFmrReportData zfrd
        JOIN 
            FmrTables.tblZeissFmrReportHeader zfrh ON zfrd.report_id = zfrh.report_id
        JOIN 
            FmrTables.tblIndents ind ON zfrh.IndentID = ind.IndentID
        JOIN 
            NirTables.tblOrganizations org ON ind.MachineID = org.OrganizationID
        WHERE
            ind.CreatedDateTime >= '{start_date_dt.strftime('%Y-%m-%d')}'
            AND ind.CreatedDateTime < '{end_date_dt_exclusive.strftime('%Y-%m-%d')}'
        ;
        """

        try:
            df = pd.read_sql(query, engine)
            print(f"✅ Rows fetched: {len(df)}")
            # Debug: show distinct names
            print("Distinct feature names:", df['name'].dropna().unique())
        except Exception as e:
            context['error'] = f"❌ Database error: {e}"
            return render(request, 'fmr_project/home.html', context)
        finally:
            engine.dispose()

        if df.empty:
            context['error'] = "⚠️ No data available for the selected date range."
            return render(request, 'fmr_project/home.html', context)

        # Clean
        df['OrganizationName'] = df['OrganizationName'].astype(str).str.strip().str.upper()
        df['name'] = (
            df['name']
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(r'\s+', ' ', regex=True)
        )
        df['CreatedDateTime'] = pd.to_datetime(df['CreatedDateTime'])
        df = df.sort_values(['OrganizationName', 'CreatedDateTime'])

        # Limits
        df['UL'] = df['nominal'] + df['usl']
        df['LL'] = df['nominal'] + df['lsl']
        df.drop(columns=['nominal', 'usl', 'lsl'], inplace=True, errors=True)

        # Chuck assignment
        def assign_chuck_no(group):
            vals = ['Chuck 1', 'Chuck 2']
            return [vals[i % 2] for i in range(len(group))]

        df['Chuck no'] = (
            df.groupby('OrganizationName', group_keys=False)
              .apply(lambda g: pd.Series(assign_chuck_no(g), index=g.index))
        )

        # Aggregate duplicates
        df_agg = df.groupby(
            ['CreatedDateTime', 'OrganizationName', 'Chuck no', 'drawing_no', 'name'],
            as_index=False
        )['actual'].mean()

        # Pivot
        df_pivot = df_agg.pivot(
            index=['CreatedDateTime', 'OrganizationName', 'Chuck no', 'drawing_no'],
            columns='name',
            values='actual'
        ).reset_index()

        # Rename
        rename_map = {
            'OrganizationName': 'Machine',
            'drawing_no': 'Part no',
            "pt (pt)": "Pt",
            "rz (rz)": "Rz",
            "rmax (rmax)": "Rmax",
            "wt (wt)": "Wt",
            "straightness (straightness)": "Seat Straightness",
            "roundness (roundness_on_cone_p26)": "Guide Roundness",
            "straightness (line_p_03_2_90.0)": "Guide Straightness",
            "parallelism lines (parallelism_p4_0_180)": "Parallelism",
            "radial run out (radial_runout_on_seat_p25)": "Seat Radial Run out",
            "roundness (roundness_cone_z)": "Roundness cone Z"
        }
        df_pivot.rename(columns={k: v for k, v in rename_map.items() if k in df_pivot.columns}, inplace=True)

        # Multiply micron columns
        for col in ["Guide Roundness", "Guide Straightness", "Parallelism", "Seat Radial Run out", "Roundness cone Z"]:
            if col in df_pivot.columns:
                df_pivot[col] = df_pivot[col].astype(float) * 1000

        # Fill missing with blank (so Excel shows blank instead of NaN)
        df_pivot = df_pivot.fillna("")

        # Format date & add SR NO
        df_pivot['Date'] = pd.to_datetime(df_pivot['CreatedDateTime']).dt.strftime('%d-%b-%y')
        df_pivot.insert(0, 'SR NO', range(1, len(df_pivot) + 1))

        # Final columns (your requested format)
        final_cols = [
            'SR NO', 'Date', 'Machine', 'Chuck no',
            'Pt', 'Rz', 'Rmax', 'Wt',
            'Seat Straightness', 'Guide Roundness',
            'Guide Straightness', 'Parallelism', 'Seat Radial Run out'
        ]
        final_cols = [c for c in final_cols if c in df_pivot.columns]
        df_pivot = df_pivot[final_cols]

        print("✅ Final column list:", final_cols)

        # Excel export
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_pivot.to_excel(writer, index=False, sheet_name='Machine Data')
            workbook = writer.book
            worksheet = writer.sheets['Machine Data']

            fmt_header = workbook.add_format({
                'bold': True, 'text_wrap': True,
                'valign': 'center', 'fg_color': '#CCE5FF',
                'font_color': 'black', 'border': 1
            })
            for idx, col in enumerate(df_pivot.columns):
                worksheet.write(0, idx, col, fmt_header)
                width = max(df_pivot[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(idx, idx, width)
            worksheet.freeze_panes(1, 0)

        output.seek(0)
        filename = f"Machine_Data_{start_date}_{end_date}.xlsx"
        response = HttpResponse(output.read(),
                                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename={filename}'
        return response

    return render(request, 'fmr_project/home.html', context)
