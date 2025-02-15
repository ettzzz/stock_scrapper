import pandas as pd
import numpy as np

import torch
import torch.nn as nn


class StockActionPredictor(nn.Module):
    def __init__(self, patch_size, embed_dim, num_classes):
        super(StockActionPredictor, self).__init__()
        self.patch_size = patch_size ## each days feature len
        self.embed_dim = embed_dim ## steps
        self.positional_encoding = nn.Parameter(torch.randn(1, patch_size, embed_dim))
        encoder_layers = nn.TransformerEncoderLayer(d_model=embed_dim, nhead=7)
        self.encoder = nn.TransformerEncoder(encoder_layer=encoder_layers, num_layers=6)

        self.fc = nn.Linear(embed_dim, num_classes)

    def forward(self, x):
        x = x.transpose(1, 2)
        x += self.positional_encoding
        x = self.encoder(x)
        x = x.mean(dim=1)
        x = self.fc(x)
        return x

if __name__ == "__main__":
    step_size = 63
    feature_len = 9
    batch_size = 8
    model = StockActionPredictor(patch_size=feature_len, embed_dim=step_size, num_classes=2)

    x = torch.randn((batch_size, feature_len, step_size))
    model.eval()
    with torch.no_grad():
        output = model(x)  # 传入输入序列
        actions = torch.argmax(output, dim=-1)
        action_labels = ["买入", "不操作"]
        predicted_actions = [action_labels[action.item()] for action in actions]
        print(output)
        print(predicted_actions)