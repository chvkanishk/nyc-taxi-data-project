# Create simple architecture diagram
import matplotlib.pyplot as plt
import matplotlib.patches as patches

fig, ax = plt.subplots(figsize=(14, 8))

# Define boxes
boxes = [
    {"name": "NYC TLC\nData", "pos": (0.5, 3.5), "color": "#3498db"},
    {"name": "Bronze\nLayer", "pos": (2.5, 3.5), "color": "#95a5a6"},
    {"name": "Silver\nLayer", "pos": (4.5, 3.5), "color": "#bdc3c7"},
    {"name": "Gold\nLayer", "pos": (6.5, 3.5), "color": "#f39c12"},
    {"name": "Dashboard", "pos": (8.5, 4.5), "color": "#2ecc71"},
    {"name": "ML Model", "pos": (8.5, 2.5), "color": "#e74c3c"},
]

# Draw boxes
for box in boxes:
    rect = patches.Rectangle(
        (box["pos"][0]-0.6, box["pos"][1]-0.4),
        1.2, 0.8,
        linewidth=2, edgecolor='black',
        facecolor=box["color"], alpha=0.7
    )
    ax.add_patch(rect)
    ax.text(box["pos"][0], box["pos"][1], box["name"],
            ha='center', va='center', fontsize=10, weight='bold')

# Draw arrows
arrows = [
    ((1.1, 3.5), (1.9, 3.5)),
    ((3.1, 3.5), (3.9, 3.5)),
    ((5.1, 3.5), (5.9, 3.5)),
    ((7.1, 3.5), (7.9, 4.5)),
    ((7.1, 3.5), (7.9, 2.5)),
]

for start, end in arrows:
    ax.annotate('', xy=end, xytext=start,
                arrowprops=dict(arrowstyle='->', lw=2, color='black'))

ax.set_xlim(0, 10)
ax.set_ylim(1, 5.5)
ax.axis('off')
ax.set_title('NYC Taxi Data Pipeline Architecture', fontsize=16, weight='bold', pad=20)

plt.tight_layout()
plt.savefig('docs/architecture.png', dpi=300, bbox_inches='tight', facecolor='white')
print("✅ Architecture diagram saved!")