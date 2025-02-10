// visualization.js
class DashboardVisualizer {
    constructor() {
        this.colors = {
            primary: '#6366f1',
            success: '#22c55e',
            danger: '#ef4444',
            warning: '#f59e0b',
            gray: '#6b7280'
        };

        this.setupErrorHandling();
        this.setupWebSocket();
    }

    setupErrorHandling() {
        this.showError = (message) => {
            const errorContainer = document.getElementById('error-container');
            errorContainer.innerHTML = `
                <div class="p-4 bg-red-50 border border-red-200 rounded-xl flex items-center">
                    <svg class="w-5 h-5 text-red-500 mr-3" fill="currentColor" viewBox="0 0 20 20">
                        <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd"/>
                    </svg>
                    <div class="text-sm text-red-600">${message}</div>
                </div>
            `;
            errorContainer.classList.remove('hidden');
        };
    }

    setupWebSocket() {
        const socket = io('http://localhost:5000');

        socket.on('connect', () => {
            console.log('Connected to WebSocket');
        });

        socket.on('real_time_data', (data) => {
            this.updateDashboard(data);
        });

        socket.on('error', (error) => {
            this.showError('WebSocket connection error');
        });
    }

    async fetchInitialData() {
        try {
            const response = await fetch("http://127.0.0.1:5000/api/sentiment");
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();

            if (data.error) throw new Error(data.error);
            this.updateDashboard(data);
            this.translateTextInDocument();
        } catch (error) {
            console.error("Error:", error);
            this.showError(error.message || "Failed to load data. Please try again later.");
        }
    }

    updateDashboard(data) {
        this.drawTable(data.trending);
        this.drawBarChart(data.trending);
        this.drawPieChart(data.insights);
        this.drawScatterPlot(data.trending);
    }

    drawTable(trending) {
        const tbody = document.getElementById("trending-table");

        if (Array.isArray(trending) && trending.length > 0) {
            tbody.innerHTML = trending.slice(0, 7).map((item) => `
                <tr class="hover:bg-gray-50 transition-colors">
                    <td class="py-3 text-center">
                        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium 
                            ${item.mean_score > 0.6 ? 'bg-green-100 text-green-800' :
                    item.mean_score > 0.4 ? 'bg-yellow-100 text-yellow-800' : 'bg-red-100 text-red-800'}">
                            ${(item.mean_score).toFixed(1)}
                        </span>
                    </td>
                    <td class="py-3 text-center">${item.times_mentioned.toLocaleString()}</td>
<td class="py-3 font-medium text-gray-900 text-right">${item.topic}</td>
                </tr>
            `).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="3" class="py-3 text-center text-gray-500">No trending data available</td></tr>';
        }
    }

    drawPieChart(insights) {
        const svg = d3.select("#pie-chart");
        svg.selectAll("*").remove();

        const width = svg.node().getBoundingClientRect().width;
        const height = 300;
        const radius = Math.min(width, height) / 2 - 20;

        // Transform insights data for D3
        const colorMapping = {
            negative: this.colors.danger,
            neutral: this.colors.gray,
            positive: this.colors.success
        };

        const data = Object.entries(insights).map(([name, values]) => ({
            name,
            percentage: values.percentage,
            total: values.total,
            color: colorMapping[name] // Assign correct color
        }));

        const pie = d3.pie()
            .value(d => d.percentage)
            .sort(null);

        const arc = d3.arc()
            .innerRadius(radius * 0.5)
            .outerRadius(radius);

        const g = svg.append("g")
            .attr("transform", `translate(${width / 2},${height / 2})`);

        // Create tooltip
        const tooltip = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);

        // Create pie segments
        const arcs = g.selectAll("arc")
            .data(pie(data))
            .enter()
            .append("g")
            .attr("class", "arc");

        arcs.append("path")
            .attr("d", arc)
            .attr("fill", d => d.data.color)
            .attr("stroke", "white")
            .attr("stroke-width", 2)
            .style("transition", "transform 0.2s")
            .on("mouseover", function (event, d) {
                d3.select(this)
                    .transition()
                    .duration(200)
                    .attr("transform", "scale(1.05)");

                tooltip.transition()
                    .duration(200)
                    .style("opacity", 1);

                tooltip.html(`
                    <div class="p-2">
                        <div>${d.data.total.toLocaleString()} : مجموع</div>
                        <div>%${d.data.percentage.toFixed(1)} : نسبة</div>
                    </div>
                `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 10) + "px")
                    .style("visibility", "visible");
            })
            .on("mouseout", function () {
                d3.select(this)
                    .transition()
                    .duration(200)
                    .attr("transform", "scale(1)");

                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0)
                    .style("visibility", "hidden");
            });

        // Add percentage labels
        arcs.append("text")
            .attr("transform", d => `translate(${arc.centroid(d)})`)
            .attr("dy", ".35em")
            .style("text-anchor", "middle")
            .style("fill", "white")
            .style("font-size", "14px")
            .style("font-weight", "bold")
            .text(d => `${d.data.percentage.toFixed(1)}%`);
    }

    drawScatterPlot(trending) {
        const svg = d3.select("#scatter-plot");
        svg.selectAll("*").remove();

        const margin = { top: 40, right: 40, bottom: 60, left: 60 };
        const width = svg.node().getBoundingClientRect().width - margin.left - margin.right;
        const height = svg.node().getBoundingClientRect().height - margin.top - margin.bottom;

        // Create scales
        const xScale = d3.scaleLinear()
            .domain([0, d3.max(trending, d => d.score_sum)])
            .range([0, width])
            .nice();

        const yScale = d3.scaleLinear()
            .domain([0, d3.max(trending, d => d.positive_count)])
            .range([height, 0])
            .nice();

        // Create chart group
        const g = svg.append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

        // Add gridlines
        g.append("g")
            .attr("class", "grid")
            .call(d3.axisLeft(yScale)
                .tickSize(-width)
                .tickFormat("")
            )
            .style("stroke-dasharray", "3,3")
            .style("stroke-opacity", 0.1);

        g.append("g")
            .attr("class", "grid")
            .attr("transform", `translate(0,${height})`)
            .call(d3.axisBottom(xScale)
                .tickSize(-height)
                .tickFormat("")
            )
            .style("stroke-dasharray", "3,3")
            .style("stroke-opacity", 0.1);

        // Add axes
        g.append("g")
            .attr("class", "x-axis")
            .attr("transform", `translate(0,${height})`)
            .call(d3.axisBottom(xScale))
            .append("text")
            .attr("class", "axis-label")
            .attr("x", width / 2)
            .attr("y", 45)
            .attr("fill", "currentColor")
            .style("text-anchor", "middle")
            .text("مجموع النقاط");

        g.append("g")
            .attr("class", "y-axis")
            .call(d3.axisLeft(yScale))
            .append("text")
            .attr("class", "axis-label")
            .attr("transform", "rotate(-90)")
            .attr("x", -height / 2)
            .attr("y", -45)
            .attr("fill", "currentColor")
            .style("text-anchor", "middle")
            .text("عدد التعليقات الإيجابية");

        // Create tooltip
        const tooltip = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);

        // Add scatter points
        g.selectAll("circle")
            .data(trending)
            .enter()
            .append("circle")
            .attr("cx", d => xScale(d.score_sum))
            .attr("cy", d => yScale(d.positive_count))
            .attr("r", 8)
            .attr("fill", this.colors.primary)
            .attr("opacity", 0.7)
            .attr("stroke", "white")
            .attr("stroke-width", 2)
            .on("mouseover", (event, d) => {
                d3.select(event.currentTarget)
                    .transition()
                    .duration(200)
                    .attr("r", 10)
                    .attr("opacity", 1);

                tooltip.transition()
                    .duration(200)
                    .style("opacity", 1);

                tooltip.html(`
                    <div class="p-2">
                        <div class="font-bold">${d.topic}</div>
                        <div>مجموع النقاط: ${d.score_sum.toFixed(1)}</div>
                        <div>التعليقات الإيجابية: ${d.positive_count}</div>
                    </div>
                `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 10) + "px")
                    .style("visibility", "visible");
            })
            .on("mouseout", (event) => {
                d3.select(event.currentTarget)
                    .transition()
                    .duration(200)
                    .attr("r", 8)
                    .attr("opacity", 0.7);

                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0)
                    .style("visibility", "hidden");
            });
    }

    drawBarChart(trending) {
        const svg = d3.select("#bar-chart");
        svg.selectAll("*").remove();

        const top5Data = trending
            .sort((a, b) => b.times_mentioned - a.times_mentioned)
            .slice(0, 10);

        // Transform data for stacking
        const stackedData = top5Data.map(d => ({
            topic: d.topic,
            positive: d.positive_count,
            negative: d.negative_count,
            neutral: d.neutral_count
        }));

        const margin = { top: 30, right: 80, bottom: 70, left: 60 };
        const width = svg.node().getBoundingClientRect().width - margin.left - margin.right;
        const height = svg.node().getBoundingClientRect().height - margin.top - margin.bottom;

        // Set up scales
        const x = d3.scaleBand()
            .range([0, width])
            .domain(stackedData.map(d => d.topic))
            .padding(0.2);

        // Stack the data
        const keys = ['positive', 'negative', 'neutral'];
        const stack = d3.stack().keys(keys);
        const stackedValues = stack(stackedData);

        const y = d3.scaleLinear()
            .domain([0, d3.max(stackedValues[stackedValues.length - 1], d => d[1])])
            .nice()
            .range([height, 0]);

        // Set up colors
        const colors = {
            positive: this.colors.success,  // success green
            negative: this.colors.danger, // danger red
            neutral: this.colors.gray    // gray
        };

        const g = svg.append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

        // Add gridlines
        g.append("g")
            .attr("class", "grid")
            .call(d3.axisLeft(y)
                .tickSize(-width)
                .tickFormat("")
            )
            .style("stroke-dasharray", "3,3")
            .style("stroke-opacity", 0.1);

        // Add axes
        g.append("g")
            .attr("transform", `translate(0,${height})`)
            .call(d3.axisBottom(x))
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-45)");

        g.append("g")
            .call(d3.axisLeft(y));

        // Create tooltip
        const tooltip = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);

        // Add stacked bars
        const layer = g.selectAll("layer")
            .data(stackedValues)
            .enter()
            .append("g")
            .attr("fill", (d, i) => colors[keys[i]]);

        layer.selectAll("rect")
            .data(d => d)
            .enter()
            .append("rect")
            .attr("x", d => x(d.data.topic))
            .attr("y", d => y(d[1]))
            .attr("height", d => y(d[0]) - y(d[1]))
            .attr("width", x.bandwidth())
            .on("mouseover", (event, d) => {
                const value = d[1] - d[0];

                tooltip.transition()
                    .duration(200)
                    .style("opacity", 1);

                tooltip.html(`
                <div class="p-2">
                    <div>${value.toLocaleString()}</div>
                </div>
            `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 10) + "px")
                    .style("visibility", "visible");
            })
            .on("mouseout", () => {
                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0)
                    .style("visibility", "hidden");
            });

        // Add legend
        const legend = svg.append("g")
            .attr("font-family", "sans-serif")
            .attr("font-size", 10)
            .attr("text-anchor", "start")
            .selectAll("g")
            .data(keys.slice().reverse())
            .enter().append("g")
            .attr("transform", (d, i) => `translate(${width + margin.left + 10},${margin.top + i * 20})`);

        legend.append("rect")
            .attr("x", 0)
            .attr("width", 15)
            .attr("height", 15)
            .attr("fill", d => colors[d]);

        legend.append("text")
            .attr("x", 20)
            .attr("y", 7.5)
            .attr("dy", "0.32em")
            .text(d => d);
    }

    translateTextInDocument() {
        const translations = {
            positive: "إيجابي",
            neutral: "محايد",
            negative: "سلبي"
        };

        d3.selectAll("text") // Select all text elements in the chart
            .each(function () {
                let textElement = d3.select(this);
                let text = textElement.text();

                // Replace case-insensitive occurrences
                Object.keys(translations).forEach(key => {
                    let regex = new RegExp(key, "gi"); // Case-insensitive replacement
                    if (regex.test(text)) {
                        textElement.text(text.replace(regex, translations[key]));
                    }
                });
            });
    }

}

// Initialize dashboard
document.addEventListener('DOMContentLoaded', () => {
    const dashboard = new DashboardVisualizer();
    dashboard.fetchInitialData();
});