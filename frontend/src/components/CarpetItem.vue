<template>
  <div class="carpet-container">


    <div class="col-sm-6 table-responsive">
      <table class="email-table">
        <thead>
          <tr>
            <th scope="col"></th>
            <th scope="col">De</th>
            <th scope="col">Para</th>
            <th scope="col">Titulo</th>
          </tr>
        </thead>
        <tbody>
          <!-- <tr v-for="(item, index) in records" v-bind:class="{ 'table-active': isSelected(index) }"
              @click="select(index)">
              <td>{{ pagesPrevious + index + 1 }}</td>
              <td>{{ item._source.From }}</td>
              <td>{{ item._source.To }}</td>
              <td>{{ item._source.Subject }}</td>
            </tr> -->

          <tr>
            <td>1</td>
            <td> item._source.From </td>
            <td> item._source.To </td>
            <td> item._source.Subject </td>
          </tr>
          <tr>
            <td>2</td>
            <td> item._source.From </td>
            <td> item._source.To </td>
            <td> item._source.Subject </td>
          </tr>
          <tr>
            <td>3</td>
            <td> item._source.From </td>
            <td> item._source.To </td>
            <td> item._source.Subject </td>
          </tr>

        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.carpet-container {
  padding: 1em;
}

.email-table {
  background-color: white;
  text-align: left;
  border-collapse: collapse;
  width: 80%;
}

th,
td {
  border: solid 1px #ce93d8;
  padding: 12px;
}

thead {
  background-color: #ab47bc;
  border-bottom: solid 5px #ce93d8;
  color: #ffffff;
}

tr:nth-child(even) {
  background-color: #ddd;
}
</style>
