from strands import Agent
from strands.models import BedrockModel

# Use the APAC inference profile model ID so it works in ap-northeast-1
BEDROCK_MODEL_ID = "apac.anthropic.claude-3-sonnet-20240229-v1:0"

# Define the Bedrock model for Strands to use
bedrock_model = BedrockModel(
    model_id=BEDROCK_MODEL_ID,
    temperature=0.3,
    region_name="ap-northeast-1",  # our region
)

# Simple test task
task = "write Python code to differentiate an equation"

agent = Agent(
    model=bedrock_model,
    system_prompt="you are a helpful coding assistant",
)

response = agent(task)
print(response)
