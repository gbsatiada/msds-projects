import streamlit as st
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
import geopandas as gpd
import matplotlib.pyplot as plt
# import streamlit as st

# Custom CSS to inject contained in a string
custom_css = """
    <style>
        .big-font {
            font-size:30px !important;
        }
        .stButton>button {
            color: white;
            background-color: #FF4B4B;
            border-radius:10px;
            border:none;
            padding: 10px 24px;
            font-size: 18px;
        }
        .stTextInput>div>div>input {
            margin-bottom: 10px;
        }
    </style>
"""

st.set_option('deprecation.showPyplotGlobalUse', False)


def train_model():
    df_condo_final = pd.read_csv('cleaned_housing.csv')
    X_final = df_condo_final.drop('Price (PHP)',axis=1)
    y_final = df_condo_final['Price (PHP)']

    price_bins_final = pd.qcut(df_condo_final['Price (PHP)'], q=3, labels=['low', 'medium','high'])
    X_train_final, X_test_final, y_train_final, y_test_final = train_test_split(X_final,
                                                                                y_final,
                                                                        stratify=price_bins_final,
                                                                                test_size=0.2,
                                                                                random_state=13)
    gbr_final = GradientBoostingRegressor(learning_rate=0.2,
                                          n_estimators=400, 
                                          random_state=13)
    gbr_final.fit(X_train_final, y_train_final)
    return gbr_final
    
    
def get_all_cities():
    feature_df = pd.read_csv('feature_df.csv')
    city_list = feature_df['city'].unique().tolist()
    return city_list

def get_brgy_for_city(city_name):
    feature_df = pd.read_csv('feature_df.csv')
    brgy_list = feature_df[feature_df['city']==city_name]['brgy'].unique().tolist()
    return brgy_list

def get_features(bedrooms, bath, fa, barangay, city):
    feature_df = pd.read_csv('feature_df.csv')
    feature_df.fillna(0,inplace=True)
    # feature_df
    filtered_df = feature_df.query(f"`brgy`=='{barangay}' & `city`=='{city}'").copy()
    filtered_df['Bedrooms'] = bedrooms
    filtered_df['Bath'] = bath
    filtered_df['Floor_area (sqm)'] = fa
    # filtered_df['Price (PHP)'] = ""
    filtered_df["city_pop"] = filtered_df["city_pop"].str.replace(",", "")
    filtered_df = filtered_df.apply(pd.to_numeric, errors="coerce")
    
    final_columns = [
        'Latitude', 'Longitude', 'Bedrooms', 'Bath',
        'Floor_area (sqm)', 'brgy_area_sqm', 'city_pop',
        'Food_Count', 'Education_Count', 'Healthcare_Count',
        'Public_Services_Count', 'Finance_Count', 'Transportation_Count',
        # 'Price (PHP)'
    ]
    return filtered_df[final_columns]

def get_price(sample_1, model):
    sample_1['Price (PHP)'] = (model.predict(sample_1))[0]*1.4
    return sample_1

def plot_unit(city, barangay):
    mm_brgy_centroid = gpd.read_file('mm_brgy_centroid.shp')
    ph_shp = gpd.read_file('ph_shp.shp')
    city_shp = gpd.read_file('city_level_shp.shp')

    mm_brgy_centroid.columns = ['city', 'barangay', 'geometry']
    ph_shp.columns = ['province', 'muni/city', 'geometry']
    city_shp.columns = ['city', 'city_area_ha', 'geometry']
    
    unit_city_loc = city_shp.query(f"`city`=='{city}'")
    unit_brgy_loc = mm_brgy_centroid.query(f"`barangay`=='{barangay}' & `city`=='{city}'")

    # Plotting
    fig, ax = plt.subplots(figsize=(15,10))
    
    unit_brgy_loc.plot(ax=ax, marker='^', zorder=4, markersize=30, color='white')
    unit_brgy_loc.plot(ax=ax, marker='^', zorder=4,
                       markersize=18, color='maroon', legend=True,
                       label=f"Unit Location: {barangay}, {city}")


    unit_city_loc.plot(ax=ax, color='#FFDC4D', linewidth=0.6, edgecolor='black', zorder=3)#0F2D66


    city_shp.plot(ax=ax, color='gainsboro', edgecolor='black', linewidth=0.3, zorder=2)#FFDC4D
    xlims = plt.xlim()
    ylims = plt.ylim()
    ph_shp.boundary.plot(ax=ax, color='darkgrey', linewidth=0.6, zorder=1)
    plt.xlim(xlims)
    plt.ylim(ylims)

    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    # Adjust the legend
    leg = plt.legend(prop={'size': 9}, loc='upper left')
    leg.set_zorder(4)  # put the legend on top

    plt.show()


# Initialize your app with the trained model
model = train_model()

# Streamlit app layout
def main():
    # Display an image from the local disk
    st.image('title_banner_2.jpg')
    st.markdown(custom_css, unsafe_allow_html=True)
    
    # Use the new styles in your app
    st.markdown('<p class="big-font">Condo Price Prediction App</p>', unsafe_allow_html=True)
    
    # Inputs
    fa = st.number_input('Floor Area in (m2)', format="%.2f", value=24.00)
    bedrooms = st.number_input('Number of Bedrooms (For Studio, input 1)', step=1, format="%d",value=1)
    bath = st.number_input('Number of Bathrooms', step=1, format="%d",value=1)
    city = st.selectbox('City', get_all_cities())
    brgy = st.selectbox('Barangay', get_brgy_for_city(city))

    # Button to make prediction
    if st.button('Predict Price'):
        # Ensure the model is loaded
        if bath < 1 or bedrooms < 1 or fa <1:
            st.error("Invalid Input. Input must be at least 1.")
            
        elif model is not None:
            features = get_features(bedrooms, bath, fa, brgy, city)
            prediction = get_price(features, model)
            
            if bedrooms==1 and bath==1:
                st.write(f"The estimated price for a {bedrooms} bedroom "
                     f"and {bath} bathroom in {brgy}, {city} is: "
                         f"{prediction['Price (PHP)'].iloc[0]:,.2f} PHP")
            elif bedrooms>1 and bath==1:
                st.write(f"The estimated price for a {bedrooms} bedrooms "
                     f"and {bath} bathroom in {brgy}, {city} is: "
                         f"{prediction['Price (PHP)'].iloc[0]:,.2f} PHP")
            elif bedrooms==1 and bath>1:
                st.write(f"The estimated price for a {bedrooms} bedroom "
                     f"and {bath} bathrooms in {brgy}, {city} is: "
                         f"{prediction['Price (PHP)'].iloc[0]:,.2f} PHP")
            else:
                st.write(f"The estimated price for a {bedrooms} bedrooms "
                     f"and {bath} bathrooms in {brgy}, {city} is: "
                         f"{prediction['Price (PHP)'].iloc[0]:,.2f} PHP")
                
            fig = plot_unit(city, brgy)
            st.pyplot(fig)
            
            del fig, fa, bedrooms, bath, city, brgy, features, prediction

        else:
            st.error("Model is not loaded properly.")

if __name__ == "__main__":
    main()