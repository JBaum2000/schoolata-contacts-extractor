"""
Centralised, semi‑stable LinkedIn selectors.  
Many use `aria-label`, `role` or `data-test-*` attributes that rarely change.
"""
from selenium.webdriver.common.by import By

class Selectors:
    SEARCH_BOX = (By.CSS_SELECTOR, 'input[role="combobox"][aria-label="Search"]')
    PILL_PEOPLE = (By.XPATH, '//button[normalize-space()="People"]')
    PILL_CURRENT_COMPANY = (By.XPATH, '//button[@aria-label="Current company filter. Clicking this button displays all Current company filter options."]')
    CURRENT_COMPANY_LIST = (By.CSS_SELECTOR, 'ul.search-reusables__collection-values-container li')
    CURRENT_COMPANY_LABEL = (By.CSS_SELECTOR, 'label span.t-14')
    SHOW_RESULTS = (By.XPATH, '//span[normalize-space()="Show results"]/ancestor::button')
    RESULT_LINKS = (By.CSS_SELECTOR, 'a[data-test-app-aware-link][href*="/in/"]')
    PAGINATION_STATE = (By.CSS_SELECTOR, 'div.artdeco-pagination__page-state, span.artdeco-pagination__page-state')
    PAGINATION_NEXT = (By.XPATH, '//button[@aria-label="Next" and not(@disabled)]')
    MAIN_TEXT = (By.CSS_SELECTOR, 'main')
    CONTACT_INFO_BTN = (By.ID, 'top-card-text-details-contact-info')
    CONTACT_MODAL = (By.CSS_SELECTOR, 'div.artdeco-modal')
    CONTACT_MODAL_BODY = (By.CSS_SELECTOR, 'div.pv-profile-section__section-info')
    CONTACT_MODAL_UPSELL = (By.CSS_SELECTOR, 'div.card-upsell-v2__text-container')
    CONTACT_MODAL_CLOSE = (By.CSS_SELECTOR, 'button.artdeco-modal__dismiss')
    PILL_CURRENT_COMPANY = (By.ID, "searchFilter_currentCompany")

