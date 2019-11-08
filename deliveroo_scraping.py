import urllib.request
import pandas as pd
import sqlite3
import re
from bs4 import BeautifulSoup


# This is so that Deliveroo think the scraper is Google Chrome
# as opposed to a web scraper
hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11' +
       '(KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*' +
       ';q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}


def process_menu(doc, url, tags_df, tag_type, restaurants, restaurants_to_tags,
                 menu_sections, menu_items):
    # This function processes the menu

    # This gets the restaurant_name by finding the <h1> tag with the CSS class
    # restaurant_name
    restaurant_name = doc.find("h1", class_="restaurant__name", text=True).text

    # This gets the deliveroo_name by selecting the appropriate part from the
    # URL
    # This will fail on restaurants not in London
    deliveroo_name = re.findall(
        '(?<=https://deliveroo.co.uk/menu/london/)(.*)(?=\\?postcode=)',
        url)[0]

    # This adds this to the restaurants dataframe
    # This isn't very efficient, if you were wanting to scrape large numbers
    # you wouldn't want to use .append
    restaurants = restaurants.append(
        {"name": restaurant_name, "deliveroo_name": deliveroo_name},
        ignore_index=True)

    # This gets the restaurant_id by finding the index of what as inserted
    # Again this isn't very efficient
    restaurant_id = restaurants[
        (restaurants == [restaurant_name, deliveroo_name]).all(
            axis=1)].index[0]
    restaurant_tags = []

    # Deal with tags
    # Start by finding all <small> tags with the CSS class tag
    for tag in doc.find_all("small", class_="tag"):
        # The second element of the <small> CSS class is the type of the tag
        # this could be locale or food etc.
        tagtype = tag['class'][1]
        # The name of the tag is what is inside the <small>
        name = tag.text

        # See if the tagtype exists in the tag_type dataframe
        type_matches = tag_type[(tag_type == [tagtype]).all(axis=1)]

        # If it doesn't
        if len(type_matches) == 0:
            # Add it (again not very efficient)
            tag_type = tag_type.append({"name": tagtype}, ignore_index=True)

            # Update the matches
            type_matches = tag_type[(tag_type == [tagtype]).all(axis=1)]

        # See if the tag already exists in the tags_df dataframe
        matches = tags_df[
            (tags_df == [name, type_matches.index[0]]).all(axis=1)]

        # If it doesn't
        if len(matches) == 0:
            # Add it
            entry = {"name": name, "type": type_matches.index[0]}
            tags_df = tags_df.append(entry, ignore_index=True)
            matches = tags_df[(tags_df == [name, type_matches.index[0]]).all(
                axis=1)]

        # Add the tag to a list of tags for that restaurant
        restaurant_tags.append(matches.index[0])

    # For each tag
    for tag in restaurant_tags:
        # Add this to restaurants_to_tags df
        restaurants_to_tags = restaurants_to_tags.append(
            {"restaurant_id": restaurant_id, "tag_id": tag}, ignore_index=True)

    # For each category (in the menu, e.g. Sides, Mains, Desserts, Drinks -
    # different for every restaurant though!) process the menu items
    # This is found by looking for <div> tags with the CSS class
    # menu-index-page__menu-category
    categories = doc.find_all("div", class_="menu-index-page__menu-category")
    for category in categories:
        # the category name is inside the h3 inside the div
        category_name = category.h3.text
        # Add the category to the menu_sections data frame. Again this isn't
        # efficient.
        menu_sections = menu_sections.append(
            {"restaurant_id": restaurant_id, "name": category_name},
            ignore_index=True)

        # Get the id in the menu_sections data frame
        category_id = menu_sections[
            (menu_sections == [restaurant_id, category_name]).all(
                axis=1)].index[0]

        # Get each of the items in that category
        category_items = []
        # For each menu item. Found by looking for <div> inside the category
        # with the CSS class menu-index-page__item_content
        items_html = category.find_all("div",
                                       class_="menu-index-page__item-content")
        for menu_item in items_html:
            # The name is the <h6> with the CSS class
            # menu-index-page__item-title
            item_name = \
                menu_item.find("h6", class_="menu-index-page__item-title").text

            # The price is the <span> with the CSS class
            # menu-index-page__item-price. The £ symbol is dropped, it is then
            # converted to a floating-point number (decimal), multiplied by 100
            # so that it is in pence. It is then converted to an integer.
            #
            # https://stackoverflow.com/questions/3730019/why-not-use-double-or-float-to-represent-currency
            price_as_text = \
                menu_item.find("span", class_="menu-index-page__item-price")\
                .text[1:]
            price_as_float = float(price_as_text)
            item_price = int(price_as_float * 100)

            # If an item is popular it has a <span> with the CSS class
            # menu-index-page__item-popular
            # So this tries to find it, if it exists is_item_popular = True,
            # False otherwise.
            is_item_popular = menu_item.find(
                "span", class_="menu-index-page__item-popular") is not None

            # Add this menu_item to category_items
            category_items.append(
                {"menu_section_id": category_id,
                 "name": item_name,
                 "price_in_pence": item_price,
                 "is_popular": is_item_popular}
            )

        # Add all the menu items in that category to the menu_items data frame,
        # this is more efficient than doing this one at a time
        menu_items = menu_items.append(category_items, ignore_index=True)

    # Return the updated dataframes
    return (tags_df, tag_type, restaurants, restaurants_to_tags, menu_sections,
            menu_items)


def get_restaurant_and_process_menu(url, tags_df, tag_type, restaurants,
                                    restaurants_to_tags, menu_sections,
                                    menu_items, restaurants_to_locs,
                                    postcodes):
    # This functions gets the restaurant and then processes its menu if it
    # hasn't been processed before

    # Get the deliveroo name from the url
    deliveroo_name = re.findall(
        '(?<=https://deliveroo.co.uk/menu/london/)(.*)(?=\\?postcode=)',
        url)[0]

    # If this restaurant hasn't been seen before
    if deliveroo_name not in restaurants['deliveroo_name']:
        # Get the webpage
        request = urllib.request.Request(url, headers=hdr)
        page = urllib.request.urlopen(request)
        soup = BeautifulSoup(page)
        # Try and process the menu, if it doesn't work handle it nicely
        try:
            process_menu(soup, url, tags_df, tag_type, restaurants,
                         restaurants_to_tags, menu_sections, menu_items)
        except Exception:
            print(f"Fail on {url}")

    # Get the postcode from the URL
    postcode = re.findall('(?<=\\?postcode=)(.)*', url)[0]
    # Find where it is in the postcodes data frame
    postcodes_index = (postcodes['post_code'] == postcode).index[0]

    # Find the restaurants id in the restaurants dataframe using the deliveroo
    # name
    restaurant_index = \
        (restaurants['deliveroo_name'] == deliveroo_name).index[0]

    # Add an entry to restaurants_to_locs saying that this restaurant is
    # available at this location
    restaurants_to_locs = restaurants_to_locs.append(
        {"restaurant_id": restaurant_index, "loc_id": postcodes_index},
        ignore_index=True)

    # Return the amended dataframes
    return (tags_df, tag_type, restaurants, restaurants_to_tags, menu_sections,
            menu_items, restaurants_to_locs)
