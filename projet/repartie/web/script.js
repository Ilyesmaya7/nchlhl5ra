const progressChart = new Chart(document.getElementById('progressChart'), {
    type: 'bar',
    data: {
        labels: ['Progress'],
        datasets: [{
            label: 'Job Progress (%)',
            data: [0],
            backgroundColor: '#4CAF50',
            borderColor: '#4CAF50',
            borderWidth: 1
        }]
    },
    options: {
        indexAxis: 'y',
        scales: {
            x: {
                beginAtZero: true,
                max: 100
            }
        },
        plugins: {
            legend: { display: false }
        }
    }
});

function updateDashboard() {
    fetch(`/data?t=${Date.now()}`)
        .then(response => {
            console.log('Response status:', response.status);
            console.log('Response headers:', [...response.headers.entries()]);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.text().then(text => {
                console.log('Raw /data response:', text);
                try {
                    return JSON.parse(text);
                } catch (e) {
                    console.error('Failed to parse JSON:', e);
                    throw e;
                }
            });
        })
        .then(data => {
            console.log('Parsed data:', data);
            if (!data.tasks || !data.workers || data.progress === undefined) {
                console.error('Invalid data structure:', data);
                return;
            }

            const tasksTable = document.getElementById('tasksTable');
            tasksTable.innerHTML = '';
            data.tasks.forEach(task => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td class="p-2">${task.id}</td>
                    <td class="p-2">${task.type}</td>
                    <td class="p-2">${task.status}</td>
                `;
                tasksTable.appendChild(row);
            });

            const workersTable = document.getElementById('workersTable');
            workersTable.innerHTML = '';
            data.workers.forEach(worker => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td class="p-2">${worker.id}</td>
                    <td class="p-2">${worker.tasks_assigned}</td>
                `;
                workersTable.appendChild(row);
            });

            progressChart.data.datasets[0].data[0] = data.progress;
            progressChart.update();
        })
        .catch(error => console.error('Error fetching data:', error));
}

setInterval(updateDashboard, 1000);
updateDashboard();