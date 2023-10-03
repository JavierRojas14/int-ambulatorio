import pandas as pd
from pptx import Presentation
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches


def add_dataframes_to_powerpoint(dataframes, pptx_filename):
    # Create a PowerPoint presentation
    prs = Presentation()

    for df in dataframes:
        # Create a new slide for each DataFrame
        slide = prs.slides.add_slide(prs.slide_layouts[5])  # Use a blank slide layout

        # Define table dimensions
        rows, cols = df.shape

        # Define table position and size on the slide
        left = Inches(1)
        top = Inches(1)
        width = Inches(8)
        height = Inches(3)

        # Add a table to the slide
        table = slide.shapes.add_table(rows + 1, cols, left, top, width, height).table

        # Set column widths
        col_width = width / cols
        for col_num in range(cols):
            table.columns[col_num].width = int(col_width)  # Convert to integer

        # Add column headers
        for col_num, column_name in enumerate(df.columns):
            cell = table.cell(0, col_num)
            cell.text = column_name
            cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER

        # Iterate through the DataFrame rows and columns
        for row_idx in range(rows):
            for col_idx in range(cols):
                cell = table.cell(row_idx + 1, col_idx)
                value = df.iat[row_idx, col_idx]

                # Check if the column contains text or numeric data
                if pd.api.types.is_string_dtype(
                    df.iloc[:, col_idx]
                ) or pd.api.types.is_object_dtype(df.iloc[:, col_idx]):
                    cell.text = str(value)
                    cell.text_frame.paragraphs[0].alignment = PP_ALIGN.LEFT
                elif pd.api.types.is_numeric_dtype(df.iloc[:, col_idx]):
                    cell.text = str(value)  # Format numeric data
                    cell.text_frame.paragraphs[0].alignment = PP_ALIGN.RIGHT

    # Save the PowerPoint presentation
    prs.save(pptx_filename)
