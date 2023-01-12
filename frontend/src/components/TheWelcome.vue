<script setup lang="ts">
import CarpetItem from './CarpetItem.vue'
import Email from './Email.vue'
</script>

<template>
  <div class="screen">
    <CarpetItem>
    </CarpetItem>
    <Email></Email>
  </div>

</template>

<style scoped>
.screen {
  width: 100%;
  display: grid;
  grid-template-columns: repeat(2, 1fr);
}
</style>
