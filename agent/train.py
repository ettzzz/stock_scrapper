

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import sys
sys.path.append("/workspace/stock_scrapper")

from agent.env import StockDataloader
from agent.model import StockActionPredictor

# Hyperparameters
num_epochs = 20
learning_rate = 0.001

step_size = 63
input_dim = 9
reward_size = 10
batch_size = 4

start_date="" 
end_date=""
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

environment = StockDataloader()

model = StockActionPredictor(patch_size=input_dim, embed_dim=step_size, num_classes=2).to(DEVICE)
optimizer = optim.Adam(model.parameters(), lr=learning_rate)

# Training loop
for epoch in range(num_epochs):
    model.train()  # Set the model to training mode
    total_loss = 0
    data_generator = environment.yield_batch_data(step_size, reward_size, batch_size=4, start_date=start_date, end_date=end_date)

    for S, S_, R, trace_meta in data_generator:
        src = torch.tensor(S, dtype=torch.float32).to(DEVICE)  # Shape: (seq_length, batch_size, input_dim)
        tgt = torch.tensor(S_, dtype=torch.float32).to(DEVICE)  # Shape: (seq_length, batch_size, input_dim)
        reward = torch.tensor(R, dtype=torch.float32).to(DEVICE)

        # Zero the gradients
        optimizer.zero_grad()
        logits = model(src) # logits requires_grad=True
        probs = F.softmax(logits, dim=1)  # probabilities requires_grad=True
        target = (reward > 0).float()

        loss = F.binary_cross_entropy_with_logits(logits[:, 1], target, weight=torch.abs(reward)).mean()

        total_loss += loss.item()
    
        # Backward pass and optimization
        loss.backward()
        optimizer.step()
        print(total_loss)

    print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {total_loss/len(data_generator):.4f}')


# if __name__ == "__main__":








