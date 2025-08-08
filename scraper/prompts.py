"""
Prompt template used when calling OpenAIIntegration.fetch_response().
"""

TEMPLATE = """
The following is a LinkedIn profile of an individual who works at {school_name}.

Please extract **only** the information listed below and return it **strictly as JSON** —
no Markdown, no commentary:

1. **name** – full name as it appears on their profile  
2. **title** – their current job title at {school_name}  
3. **department** – department or functional area (often absent)  
4. **email** – school-associated e-mail if available; otherwise null  
5. **phone** – phone number if available; otherwise null  
6. **linkedin_url** – the public profile URL (usually in the Contact-info modal)  
7. **bio** – a concise (≤ 5-sentence) bio that surfaces “ice-breaker” (quick) facts such as  
   • total years at the school / in the sector  
   • previous roles or promotions  
   • education & awards  
   • hobbies, passions, family mentions, etc.

**If any field is missing, put `null` for that value. Avoid hallucinations — rely only on the supplied text.**

---

### Return-format example  *(structure & style to replicate)*

```json
{{
  "name": "Jolene Bradford",
  "title": "Deputy Head of Admissions",
  "department": "Admissions",
  "email": "bradford.jolene20@dulwich.com",
  "phone": null,
  "linkedin_url": "https://www.linkedin.com/in/jolene-bradford/",
  "bio": "Birthday is June 1st. Mentions he is in her '10th year as an educator'. Got her MBA from University of Cumbria - graduating in 2023. Got his Post Graduate Certificate in Education from University College London (focus in primary education) - graduating in 2013. Previous expeience includes: 'Assistant Pincipal' at 'Dulwich College Beijing' (Apr 2022 - Jun 2023); 'Deputy Head of Primary' (Jan 2021 - Jan 2022), 'IB PYP Coordinator' (Aug 2018 - Jan 2022), 'Classroom Teacher' (Jun 2017 - Jan 2022) at 'Foshan EtonHouse International School'; 'Teacher' at 'Country Garden Group'. Recent LinkedIn post celebrating the schools diversity and creativity in a face-painting contest."
}}
```
Here is the text to analyse (profile + contact-info dump):

{text}
""".strip()