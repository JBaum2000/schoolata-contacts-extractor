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
7. **bio** – a concise (≤ 5-sentence) bio that surfaces “ice-breaker” facts such as  
   • total years at the school / in the sector  
   • previous roles or promotions  
   • education & awards  
   • hobbies, passions, family mentions, etc.

**If any field is missing, put `null` for that value. Avoid hallucinations — rely only on the supplied text.**

---

### Return-format example  *(structure & style to replicate)*

```json
{
  "name": "Jolene Bradford",
  "title": "Deputy Head of Admissions",
  "department": "Admissions",
  "email": "bradford.jolene20@dulwich.com",
  "phone": null,
  "linkedin_url": "https://www.linkedin.com/in/jolene-bradford/",
  "bio": "Jolene Bradford is an experienced admissions and marketing leader with 11 years at Dulwich College Beijing, where she rose from Community Liaison Officer to Director of Admissions and Marketing. She previously spent a decade at the Singapore Tourism Board in brand management. Armed with an MBA from Melbourne Business School and a Social Sciences degree from NUS, she brings over 20 years of sector expertise. Now preparing to return to Singapore, she’s driven by a passion for international education, community engagement, and lifelong learning."
}

Here is the text to analyse (profile + contact-info dump):

{text}
""".strip()