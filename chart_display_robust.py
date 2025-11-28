"""
Robust chart display component for PFM Compass validation Streamlit app.
Handles edge cases, validates data, and provides better user feedback.
"""

import streamlit as st
import pandas as pd
from typing import Optional, Tuple, List


def get_numeric_columns(df: pd.DataFrame) -> List[str]:
    """Return list of numeric column names from a DataFrame."""
    return [
        col for col in df.columns
        if pd.api.types.is_numeric_dtype(df[col])
    ]


def get_categorical_columns(df: pd.DataFrame) -> List[str]:
    """Return list of non-numeric (categorical/string) column names."""
    return [
        col for col in df.columns
        if not pd.api.types.is_numeric_dtype(df[col])
    ]


def validate_chart_data(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate that DataFrame is suitable for charting.
    
    Returns:
        Tuple of (is_valid, message)
    """
    if df is None:
        return False, "No data available"
    
    if df.empty:
        return False, "DataFrame is empty"
    
    if len(df.columns) < 1:
        return False, "DataFrame has no columns"
    
    numeric_cols = get_numeric_columns(df)
    if not numeric_cols:
        return False, "No numeric columns found for plotting"
    
    return True, "Data is valid for charting"


def safe_get_column(
    df: pd.DataFrame,
    col_name: Optional[str],
    fallback_cols: List[str]
) -> Optional[str]:
    """
    Safely get a column name, falling back to alternatives if needed.
    
    Args:
        df: The DataFrame to check
        col_name: The preferred column name
        fallback_cols: List of fallback column names to try
    
    Returns:
        Valid column name or None
    """
    if col_name and col_name in df.columns:
        return col_name
    
    for col in fallback_cols:
        if col in df.columns:
            return col
    
    return None


def determine_chart_columns(
    df: pd.DataFrame,
    stored_x_col: Optional[str] = None,
    stored_y_col: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Intelligently determine x and y columns for charting.
    
    Strategy:
    1. Use stored columns if valid
    2. For x: prefer categorical/string columns, then first column
    3. For y: prefer numeric columns
    
    Returns:
        Tuple of (x_col, y_col)
    """
    categorical_cols = get_categorical_columns(df)
    numeric_cols = get_numeric_columns(df)
    
    # Determine x column (typically categorical/labels)
    x_col = safe_get_column(
        df,
        stored_x_col,
        categorical_cols + list(df.columns)  # Fallback to any column
    )
    
    # Determine y column (must be numeric)
    y_col = safe_get_column(
        df,
        stored_y_col,
        numeric_cols
    )
    
    # If x and y are the same, try to pick different columns
    if x_col == y_col and len(df.columns) > 1:
        for col in df.columns:
            if col != y_col:
                x_col = col
                break
    
    return x_col, y_col


def render_chart_section():
    """
    Render the chart display section with robust error handling.
    
    This function handles:
    - Missing or invalid data gracefully
    - Column validation and smart defaults
    - Multiple chart type options
    - Clear user feedback for all states
    """
    st.markdown("---")
    st.subheader("📊 Latest Aggregated Result")
    
    # Safely get chart data from session state
    chart_df = st.session_state.get("last_chart_df")
    
    # Validate the data
    is_valid, validation_msg = validate_chart_data(chart_df)
    
    if not is_valid:
        st.info(
            "💡 **How to see charts here:**\n\n"
            "Ask a question that returns aggregated data, such as:\n"
            "- 'Show me top 5 scenarios by projected wealth'\n"
            "- 'Count scenarios grouped by status color'\n"
            "- 'Average fire percentage by age bucket'\n\n"
            "The assistant will extract tabular data and display it here."
        )
        return
    
    # Determine columns
    stored_x = st.session_state.get("chart_x_col")
    stored_y = st.session_state.get("chart_y_col")
    x_col, y_col = determine_chart_columns(chart_df, stored_x, stored_y)
    
    if y_col is None:
        st.warning(
            "Found data but couldn't identify a numeric column to plot. "
            "Showing the raw table instead."
        )
        st.dataframe(chart_df, use_container_width=True)
        return
    
    if x_col is None:
        x_col = chart_df.columns[0]  # Ultimate fallback
    
    # Create expandable options for power users
    with st.expander("⚙️ Chart Options", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            available_x_cols = list(chart_df.columns)
            x_col = st.selectbox(
                "X-axis (labels)",
                options=available_x_cols,
                index=available_x_cols.index(x_col) if x_col in available_x_cols else 0,
                key="chart_x_selector"
            )
        
        with col2:
            numeric_cols = get_numeric_columns(chart_df)
            if numeric_cols:
                y_col = st.selectbox(
                    "Y-axis (values)",
                    options=numeric_cols,
                    index=numeric_cols.index(y_col) if y_col in numeric_cols else 0,
                    key="chart_y_selector"
                )
        
        with col3:
            chart_type = st.selectbox(
                "Chart type",
                options=["Bar", "Line", "Area"],
                index=0,
                key="chart_type_selector"
            )
    
    # Display chart info
    st.caption(f"Plotting **{y_col}** by **{x_col}** ({len(chart_df)} rows)")
    
    # Prepare data for charting
    try:
        # Handle potential duplicate index values
        plot_df = chart_df[[x_col, y_col]].copy()
        
        # If x_col has duplicates, aggregate them
        if plot_df[x_col].duplicated().any():
            plot_df = plot_df.groupby(x_col, as_index=False)[y_col].sum()
        
        # Set index for plotting
        plot_df = plot_df.set_index(x_col)
        
        # Render the appropriate chart type
        chart_type = st.session_state.get("chart_type_selector", "Bar")
        
        if chart_type == "Bar":
            st.bar_chart(plot_df[y_col])
        elif chart_type == "Line":
            st.line_chart(plot_df[y_col])
        elif chart_type == "Area":
            st.area_chart(plot_df[y_col])
        else:
            st.bar_chart(plot_df[y_col])  # Default fallback
            
    except Exception as e:
        st.error(f"Error rendering chart: {str(e)}")
        st.info("Falling back to table view.")
    
    # Always show the data table below
    with st.expander("📋 View Raw Data", expanded=False):
        st.dataframe(chart_df, use_container_width=True)
        
        # Add download button
        # csv = chart_df.to_csv(index=False)
        # st.download_button(
        #     label="📥 Download as CSV",
        #     data=csv,
        #     file_name="chart_data.csv",
        #     mime="text/csv"
        # )


def render_chart_section_minimal():
    """
    Minimal version of chart rendering - simpler but still robust.
    Use this if you want less UI complexity.
    """
    st.markdown("---")
    st.subheader("📊 Latest Aggregated Result")
    
    chart_df = st.session_state.get("last_chart_df")
    
    # Early exit if no valid data
    if chart_df is None or chart_df.empty:
        st.info(
            "Ask a question that returns aggregated data "
            "(e.g., 'top 5', grouped counts) to see a chart here."
        )
        return
    
    # Get numeric columns
    numeric_cols = get_numeric_columns(chart_df)
    
    if not numeric_cols:
        st.warning("No numeric columns found. Showing raw data.")
        st.dataframe(chart_df, use_container_width=True)
        return
    
    # Determine columns
    x_col = st.session_state.get("chart_x_col") or chart_df.columns[0]
    y_col = st.session_state.get("chart_y_col")
    
    # Validate x_col exists
    if x_col not in chart_df.columns:
        x_col = chart_df.columns[0]
    
    # Validate y_col exists and is numeric
    if y_col not in numeric_cols:
        y_col = numeric_cols[0]
    
    # Display
    st.caption(f"Plotting **{y_col}** by **{x_col}**")
    
    try:
        st.bar_chart(chart_df.set_index(x_col)[y_col])
    except Exception as e:
        st.error(f"Chart error: {e}")
    
    st.dataframe(chart_df, use_container_width=True)


# Example usage / test
if __name__ == "__main__":
    st.title("Chart Component Test")
    
    # Create sample data for testing
    if st.button("Load Sample Data"):
        sample_df = pd.DataFrame({
            "status_color": ["green", "yellow", "red"],
            "count": [691200, 576960, 114240],
            "percentage": [50.0, 41.7, 8.3]
        })
        st.session_state["last_chart_df"] = sample_df
        st.session_state["chart_x_col"] = "status_color"
        st.session_state["chart_y_col"] = "count"
        st.rerun()
    
    if st.button("Clear Data"):
        st.session_state.pop("last_chart_df", None)
        st.session_state.pop("chart_x_col", None)
        st.session_state.pop("chart_y_col", None)
        st.rerun()
    
    # Render the chart section
    render_chart_section()