from openai import OpenAI

# OmniRoute కి కనెక్ట్ అయ్యేలా Client ని కాన్ఫిగర్ చేయడం
client = OpenAI(
    base_url="http://localhost:20128/v1",  # OmniRoute Base URL
    api_key="omniroute"                    # Default API Key
)

# AI ని ప్రశ్న అడగడం
response = client.chat.completions.create(
    model="auto",  # లేదా మీరు OmniRoute లో కనెక్ట్ చేసిన మోడల్ పేరు ఇవ్వవచ్చు
    messages=[
        {"role": "user", "content": "Write a python function to add two numbers"}
    ]
)

# అవుట్‌పుట్ ప్రింట్ చేయడం
print(response.choices[0].message.content)